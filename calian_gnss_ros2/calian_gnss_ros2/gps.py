"""GPS node — bridges u-blox serial data to ROS 2 topics.

Operates in three modes:
- **Disabled** - single antenna, no RTK heading.
- **Heading_Base** - base antenna in a moving-baseline pair.
- **Rover** - rover antenna receiving RTCM corrections.
"""

import math
import sys
from typing import Literal

import rclpy
from rclpy.node import Node
from sensor_msgs.msg import NavSatFix, Imu
from std_msgs.msg import Header
from pynmeagps import NMEAMessage
from pyrtcm import RTCMReader
from nmea_msgs.msg import Sentence

from calian_gnss_ros2_msg.msg import (
    CorrectionMessage,
    GnssSignalStatus,
    ReceiverHealthStatus,
)
from calian_gnss_ros2.logging import setup_node_logging
from calian_gnss_ros2.serial_module import UbloxSerial


class Gps(Node):
    """Main GPS node that reads from a u-blox antenna and publishes ROS 2 topics."""

    def __init__(
        self, mode: Literal["Disabled", "Heading_Base", "Rover"] = "Disabled"
    ) -> None:
        super().__init__("calian_gnss_gps")

        self.mode: Literal["Disabled", "Heading_Base", "Rover"] = mode

        # ---- Parameters -------------------------------------------------
        self.declare_parameter("unique_id", "")
        # Explicit serial device path (e.g. /dev/gnss_front). When set, the
        # driver binds to this port directly and skips the UBX unique_id scan
        # (which only probes ports whose description contains "Standard" and so
        # never matches a generic FTDI bridge). unique_id, if also set, is then
        # used only as a non-fatal post-connect sanity check.
        self.declare_parameter("device", "")
        self.declare_parameter("baud_rate", 230400)
        self.declare_parameter("use_corrections", True)
        self.declare_parameter("frame_id", "gps")
        # Rover only: heading→yaw publishing.  heading_offset_deg accounts for
        # the antenna mount (base→rover vector vs vehicle-forward; with
        # base=front/rover=rear the vector points aft, so 180).  The yaw
        # variance tracks the receiver's accHeading (tight when Fixed, loose at
        # Float); heading_stddev_deg is the FLOOR — we never claim better.
        self.declare_parameter("heading_offset_deg", 180.0)
        self.declare_parameter("heading_stddev_deg", 1.0)

        self.unique_id: str = (
            self.get_parameter("unique_id").get_parameter_value().string_value
        )
        self.device: str = (
            self.get_parameter("device").get_parameter_value().string_value
        )
        self.baud_rate: int = (
            self.get_parameter("baud_rate").get_parameter_value().integer_value
        )
        self.use_corrections: bool = (
            self.get_parameter("use_corrections").get_parameter_value().bool_value
        )
        self._frame_id: str = (
            self.get_parameter("frame_id").get_parameter_value().string_value
        )
        self.heading_offset_deg: float = (
            self.get_parameter("heading_offset_deg").get_parameter_value().double_value
        )
        self.heading_stddev_deg: float = (
            self.get_parameter("heading_stddev_deg").get_parameter_value().double_value
        )

        # ---- Logging (shared helper) ------------------------------------
        _, self.logger = setup_node_logging(self, f"{self.mode}_GPS")

        # ---- Serial module -----------------------------------------------
        self.ser = UbloxSerial(
            self.unique_id,
            self.baud_rate,
            self.mode,
            self.use_corrections,
            self.device,
        )

        # ---- Mode-specific publishers / subscribers ----------------------
        if self.mode == "Heading_Base":
            self.rtcm_publisher = self.create_publisher(
                CorrectionMessage, "rtcm_corrections", 100
            )
            self.base_status_publisher = self.create_publisher(
                GnssSignalStatus, "base_gps_extended", 50
            )
            # The base is the antenna directly corrected by NTRIP, so it carries
            # the best absolute position — publish it as a standard NavSatFix
            # (with covariance) for navsat_transform / robot_localization.
            self.base_gps_publisher = self.create_publisher(
                NavSatFix, "base_gps", 50
            )
            self.ser.rtcm_message_found += self.handle_rtcm_message

        elif self.mode in ("Rover", "Disabled"):
            self.gps_publisher = self.create_publisher(NavSatFix, "gps", 50)
            self.gps_status_publisher = self.create_publisher(
                GnssSignalStatus, "gps_extended", 50
            )
            if self.mode == "Rover":
                self.rtcm_subscriber = self.create_subscription(
                    CorrectionMessage,
                    "rtcm_corrections",
                    self.handle_rtcm_correction_from_base,
                    100,
                )
                # Moving-baseline heading, converted to an absolute ENU yaw and
                # published as an Imu (orientation only) for robot_localization
                # / navsat_transform.
                self.heading_publisher = self.create_publisher(Imu, "heading", 50)

        # ---- Common publishers / timers ----------------------------------
        self.health_publisher = self.create_publisher(
            ReceiverHealthStatus, "health", 50
        )
        self.create_timer(1, self.get_health_status)
        self.create_timer(1, self.get_status)

        # ---- Corrections (NTRIP / SPARTN) --------------------------------
        if self.use_corrections:
            self.create_subscription(
                CorrectionMessage, "corrections", self.handle_correction_message, 100
            )
            self.nmea_publisher = self.create_publisher(Sentence, "nmea", 100)
            self.create_timer(1, self.send_nmea_message)
            self._recent_nmea_gga: str = ""
            self.ser.nmea_message_found += self.handle_nmea_message

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _make_header(self) -> Header:
        """Create a stamped Header with the node's frame_id."""
        return Header(stamp=self.get_clock().now().to_msg(), frame_id=self._frame_id)

    # ------------------------------------------------------------------
    # Correction handling
    # ------------------------------------------------------------------

    def handle_correction_message(self, message: CorrectionMessage) -> None:
        """Forward correction data (NTRIP / SPARTN) to the antenna over serial."""
        self.logger.debug(
            f"Sending correction message: {message.message.tobytes().hex(' ')}"
        )
        self.ser.send(message.message.tobytes())

    def handle_nmea_message(self, nmea_message: NMEAMessage) -> None:
        """Cache the latest GGA sentence for NTRIP."""
        if nmea_message.identity == "GNGGA" and self.use_corrections:
            self._recent_nmea_gga = nmea_message.serialize().decode("utf-8")

    # ------------------------------------------------------------------
    # RTCM (base → rover)
    # ------------------------------------------------------------------

    def handle_rtcm_message(self, rtcm_message) -> None:
        """Called on the base (serial RX thread): forward each moving-base RTCM
        message to the rover immediately.

        No batching — correction latency directly degrades the rover's
        heading solution and slows ambiguity fixing, so we pass each message
        through the instant it is parsed.  rclpy's publish() is thread-safe,
        so publishing from the serial receive thread is fine.
        """
        self.rtcm_publisher.publish(
            CorrectionMessage(
                header=self._make_header(),
                message=rtcm_message.serialize(),
            )
        )

    def handle_rtcm_correction_from_base(self, message: CorrectionMessage) -> None:
        """Called on the rover: forward RTCM from the base topic to the antenna."""
        parsed = RTCMReader.parse(message.message)
        self.ser.send(parsed.serialize())
        self.logger.debug(f"Received RTCM message with identity: {parsed.identity}")

    # ------------------------------------------------------------------
    # Health & status
    # ------------------------------------------------------------------

    def get_health_status(self) -> None:
        """Publish antenna health status (Good / Bad)."""
        msg = ReceiverHealthStatus(
            header=self._make_header(),
            health="Good" if self.ser.get_antenna_health_status else "Bad",
        )
        self.health_publisher.publish(msg)

    def get_status(self) -> None:
        """Poll the serial module for signal status and publish NavSatFix + extended."""
        status: GnssSignalStatus = self.ser.get_status()
        status.header = self._make_header()

        if not status.valid_fix:
            return

        if self.mode in ("Rover", "Disabled"):
            self.gps_publisher.publish(self._make_navsatfix(status))
            self.gps_status_publisher.publish(status)
            # length > 0 means the moving-baseline heading is valid (heading and
            # length are populated together, only when relPosHeadingValid).
            if self.mode == "Rover" and status.length > 0.0:
                self._publish_heading(status)
            self.logger.debug(
                f"Published GPS data - Lat: {status.latitude:.6f}, "
                f"Lon: {status.longitude:.6f}"
            )
        else:
            # Base: directly NTRIP-corrected → best absolute position.
            self.base_gps_publisher.publish(self._make_navsatfix(status))
            self.base_status_publisher.publish(status)

    def _make_navsatfix(self, status: GnssSignalStatus) -> NavSatFix:
        """Build a NavSatFix (with covariance) from a GnssSignalStatus."""
        return NavSatFix(
            header=status.header,
            latitude=status.latitude,
            longitude=status.longitude,
            altitude=status.altitude,
            position_covariance=status.position_covariance,
            position_covariance_type=status.position_covariance_type,
            status=status.status,
        )

    def _publish_heading(self, status: GnssSignalStatus) -> None:
        """Publish the moving-baseline heading as an absolute-yaw Imu.

        Converts the GNSS compass bearing (0 = true North, clockwise) to a ROS
        ENU yaw (0 = East, counter-clockwise): yaw = 90 - bearing.
        heading_offset_deg folds in the antenna mount (base->rover vector vs
        vehicle-forward).  Only yaw is observed, so roll/pitch get huge variance
        and the angular-velocity / linear-acceleration covariances are flagged
        absent (leading -1) so robot_localization ignores them.

        Yaw variance comes from the receiver's accHeading (which already scales
        with Fixed vs Float), floored by heading_stddev_deg so a degraded
        heading is honestly down-weighted by the EKF.
        """
        forward_deg = status.heading + self.heading_offset_deg
        yaw = math.radians(90.0 - forward_deg)
        yaw = math.atan2(math.sin(yaw), math.cos(yaw))  # wrap to [-pi, pi]
        sigma_deg = self.heading_stddev_deg
        if status.heading_accuracy > 0.0:
            sigma_deg = max(status.heading_accuracy, self.heading_stddev_deg)
        var = math.radians(sigma_deg) ** 2

        imu = Imu(header=status.header)
        imu.orientation.z = math.sin(yaw / 2.0)
        imu.orientation.w = math.cos(yaw / 2.0)
        imu.orientation_covariance = [
            1e6, 0.0, 0.0,
            0.0, 1e6, 0.0,
            0.0, 0.0, var,
        ]
        imu.angular_velocity_covariance = [-1.0] + [0.0] * 8
        imu.linear_acceleration_covariance = [-1.0] + [0.0] * 8
        self.heading_publisher.publish(imu)

    def send_nmea_message(self) -> None:
        """Publish the most recent GGA sentence for NTRIP."""
        self.nmea_publisher.publish(
            Sentence(header=self._make_header(), sentence=self._recent_nmea_gga)
        )


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------


def main():
    rclpy.init()
    args = rclpy.utilities.remove_ros_args(sys.argv)
    gps = Gps(mode=args[1])
    try:
        rclpy.spin(gps)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        gps.logger.error(f"Unexpected error: {e}")
    finally:
        gps.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()

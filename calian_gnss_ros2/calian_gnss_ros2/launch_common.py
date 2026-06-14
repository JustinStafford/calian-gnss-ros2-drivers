"""Shared helpers for Calian GNSS launch files.

Centralises the config-path resolution, visualizer node, and NTRIP node
definitions so that the individual launch files stay DRY.
"""

import os
from launch_ros.actions import Node
from ament_index_python.packages import get_package_share_directory

_PKG = "calian_gnss_ros2"


def _share(*parts: str) -> str:
    """Resolve a path inside the installed package share directory."""
    return os.path.join(get_package_share_directory(_PKG), *parts)


def config_path() -> str:
    return _share("params", "config.yaml")


def ntrip_config_path() -> str:
    return _share("params", "ntrip.yaml")


def logs_config_path() -> str:
    return _share("params", "logs.yaml")


def gps_node(name: str, mode: str, *, remappings: list | None = None) -> Node:
    """Return a GPS Node action.

    Parameters
    ----------
    name : str
        ROS node name (e.g. ``"gps_publisher"``, ``"base"``, ``"rover"``).
    mode : str
        Operating mode passed as CLI argument (``"Disabled"``, ``"Heading_Base"``,
        or ``"Rover"``).
    remappings : list, optional
        ROS topic remappings.
    """
    return Node(
        package=_PKG,
        executable="calian_gnss_gps",
        name=name,
        output="screen",
        emulate_tty=True,
        parameters=[config_path(), logs_config_path()],
        namespace="calian_gnss",
        remappings=remappings or [],
        arguments=[mode],
    )


def _ntrip_env_overrides() -> dict:
    """Pull NTRIP connection settings from the environment, if present.

    Lets credentials live in a deployment-managed env file (e.g.
    /etc/mower/ntrip.env via systemd EnvironmentFile) instead of the committed
    ntrip.yaml — so secrets never land in the repo.  Any value set here
    overrides the matching key in ntrip.yaml (later params win).
    """
    env_map = {
        "hostname": "NTRIP_HOST",
        "port": "NTRIP_PORT",
        "mountpoint": "NTRIP_MOUNTPOINT",
        "username": "NTRIP_USERNAME",
        "password": "NTRIP_PASSWORD",
        "ntrip_version": "NTRIP_VERSION",
        "ssl": "NTRIP_SSL",
    }
    overrides = {}
    for param, env_var in env_map.items():
        value = os.environ.get(env_var)
        if not value:
            continue
        if param == "port":
            overrides[param] = int(value)
        elif param == "ssl":
            overrides[param] = value.strip().lower() in ("1", "true", "yes", "on")
        else:
            overrides[param] = value
    return overrides


def ntrip_node() -> Node:
    """Return the NTRIP client Node action."""
    return Node(
        package=_PKG,
        executable="ntrip_client",
        name="ntrip_client",
        output="screen",
        emulate_tty=True,
        # ntrip.yaml supplies non-secret defaults; env overrides supply the
        # caster host/mountpoint/credentials from the deployment env file.
        parameters=[ntrip_config_path(), logs_config_path(), _ntrip_env_overrides()],
        namespace="calian_gnss",
    )


def visualizer_node(port=8080) -> Node:
    """Return the GPS Visualizer Node action.

    Parameters
    ----------
    port : int or LaunchConfiguration
        HTTP port for the map visualizer. Defaults to 8080.
    """
    return Node(
        package=_PKG,
        executable="calian_gnss_gps_visualizer",
        name="gps_visualizer",
        output="screen",
        emulate_tty=False,
        parameters=[{"port": port}],
        namespace="calian_gnss",
    )

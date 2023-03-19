import pathlib
from omegaconf import OmegaConf
import sshtunnel

PROJECT_DIR = pathlib.Path(__file__).parent.parent.resolve()
CONFIG = OmegaConf.load(PROJECT_DIR / "config" / "config.yaml")

SERVER = sshtunnel.SSHTunnelForwarder(
    (CONFIG.KOLMOGOROV_IP, int(CONFIG.KOLMOGOROV_SSH_PORT)),
    ssh_username=CONFIG.SSH_USERNAME,
    ssh_password=CONFIG.SSH_PASSWORD,
    remote_bind_address=(
        CONFIG.CLICKHOUSE_REMOTE_IP,
        CONFIG.CLICKHOUSE_REMOTE_PORT,
    ),
)


from pyflink.datastream import CheckpointingMode


def configure_checkpoints(env):
    """
    Configura checkpoints para recuperación ante fallos.
    """

    # Habilitar checkpoints cada 60 segundos.
    env.enable_checkpointing(60000)

    checkpoint_config = env.get_checkpoint_config()

    # Garantizar procesamiento exactamente una vez.
    checkpoint_config.set_checkpointing_mode(
        CheckpointingMode.EXACTLY_ONCE
    )

    # Tiempo mínimo entre checkpoints.
    checkpoint_config.set_min_pause_between_checkpoints(30000)

    # Tiempo máximo para completar un checkpoint.
    checkpoint_config.set_checkpoint_timeout(120000)

    # Permitir un checkpoint concurrente.
    checkpoint_config.set_max_concurrent_checkpoints(1)

    # Tolerar hasta 3 fallos consecutivos de checkpoints.
    checkpoint_config.set_tolerable_checkpoint_failure_number(3)

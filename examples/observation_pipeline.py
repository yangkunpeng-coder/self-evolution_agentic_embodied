"""Push-based latest/window/snapshot/subscribe with independent bounded queues."""
from self_evolving_embodied import (
    ObservationInput, ObservationKey, ObservationRequest,
    QueueObservationPipeline, SensorStreamConfig,
)


def main() -> None:
    pipeline = QueueObservationPipeline()
    camera, imu = ObservationKey('camera', 'rgb'), ObservationKey('imu', 'imu')
    pipeline.register_stream(SensorStreamConfig(camera, 30, capacity=1800))
    pipeline.register_stream(SensorStreamConfig(imu, 100, retention_seconds=10, capacity=1000))
    subscription = pipeline.subscribe(ObservationRequest(camera))
    try:
        pipeline.push(ObservationInput(camera, b'synthetic-image', source_timestamp_ns=1, time_domain='synthetic'))
        pipeline.push(ObservationInput(imu, b'synthetic-imu', source_timestamp_ns=2, time_domain='synthetic'))
        first = subscription.read_next(timeout_seconds=0)
        latest = pipeline.latest(ObservationRequest(camera))
        window = pipeline.window(ObservationRequest(camera), duration_seconds=60)
        snapshot = pipeline.snapshot([ObservationRequest(camera), ObservationRequest(imu)])
        assert first == latest == window[0] == snapshot.get(camera)
        print('Independent subscription sequence:', first.sequence_id)
        print('Snapshot alignment:', snapshot.synchronization.value)
        print('Camera queue:', pipeline.stream_stats(camera))
    finally:
        subscription.close()
        pipeline.close()


if __name__ == '__main__':
    main()

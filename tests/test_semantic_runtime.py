from scripts.score_jobs import select_torch_device


class _CudaAvailable:
    @staticmethod
    def is_available():
        return True


class _CudaUnavailable:
    @staticmethod
    def is_available():
        return False


class _TorchCudaOn:
    cuda = _CudaAvailable()


class _TorchCudaOff:
    cuda = _CudaUnavailable()


def test_select_device_prefers_cuda_when_available():
    assert select_torch_device(torch_module=_TorchCudaOn()) == "cuda"


def test_select_device_falls_back_to_cpu():
    assert select_torch_device(torch_module=_TorchCudaOff()) == "cpu"

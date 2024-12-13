from abc import ABC, abstractmethod

class BaseClient(ABC):
    @abstractmethod
    def create_con(self, params: dict):
        pass

    @abstractmethod
    def pacs_pull(self):
        pass

    @abstractmethod
    def anonymize(self, dicom_dir: str, params: str, send_params: dict, pv_id: int):
        pass

    @abstractmethod
    def pacs_push(self):
        pass

    @abstractmethod
    def health_check(self):
        pass
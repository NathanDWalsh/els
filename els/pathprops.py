from abc import ABC, abstractmethod
from typing import Optional


class HumanPathPropertiesMixin(ABC):
    @abstractmethod
    def absolute(self):  # -> str
        pass

    @property  # type: ignore
    @abstractmethod
    def file(self):
        pass

    @property  # type: ignore
    @abstractmethod
    def dir(self):
        pass

    @property  # type: ignore
    @abstractmethod
    def parent(self):
        pass

    @property  # type: ignore
    @abstractmethod
    def name(self) -> str:
        pass

    @property  # type: ignore
    @abstractmethod
    def stem(self) -> str:
        pass

    @property  # type: ignore
    @abstractmethod
    def ext(self) -> str:
        pass

    @property
    def full_path_abs(self) -> str:
        return str(self.absolute())

    @property
    def full_path_rel(self) -> str:
        return str(self)

    @property
    def file_path_abs(self) -> str:
        return str(self.file.absolute()) if self.file else "not_file"

    @property
    def file_path_rel(self) -> str:
        return str(self.file) if self.file else "not_file"

    @property
    def folder_path_abs(self) -> str:
        return str(self.dir.absolute())

    @property
    def folder_path_rel(self) -> str:
        return str(self.dir)

    @property
    def leaf_name(self) -> Optional[str]:
        return self.name

    # @property
    # def leaf_name_dot_type(self) -> Optional[str]:
    #     if self.config and self.config.target and self.config.target.type:
    #         return f"{self.name}{self.config.target.type}"
    #     else:
    #         return self.name

    @property
    def file_name_full(self) -> str:
        return self.file.name if self.file else "not_file"

    @property
    def file_name_base(self) -> str:
        return self.file.stem if self.file else "not_file"

    @property
    def file_extension(self) -> str:
        return self.file.ext if self.file else "is_folder"

    @property
    def folder_name(self) -> str:
        return self.dir.name

    @property
    def parent_folder_name(self) -> str:
        return self.dir.parent.name if self.dir.parent else "no_parent"

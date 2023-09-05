from pathlib import Path


class PathToStringMixin:
    @property
    def str(self):
        return str(self)


class HumanPathPropertiesMixin:
    @property
    def full_path_abs(self) -> str:
        return self.absolute().str

    @property
    def full_path_rel(self) -> str:
        return self.str

    @property
    def file_path_abs(self) -> str:
        return self.file.absolute().str if self.file else None

    @property
    def file_path_rel(self) -> str:
        return self.file.str if self.file else None

    @property
    def folder_path_abs(self) -> str:
        return self.dir.absolute().str

    @property
    def folder_path_rel(self) -> str:
        return self.dir.str

    @property
    def leaf_name(self) -> str:
        return self.name

    @property
    def file_name_full(self) -> str:
        return self.file.name if self.file else None

    @property
    def file_name_base(self) -> str:
        return self.file.stem if self.file else None

    @property
    def file_extension(self) -> str:
        return self.file.ext if self.file else "folder"

    @property
    def folder_name(self) -> str:
        return self.dir.name

    @property
    def parent_folder_name(self) -> str:
        return self.dir.parent.name


# class BasePathMixin:
#     _base_dir = Path()  # Default to current directory

#     @classmethod
#     def set_base(cls, path: Path):
#         cls._base_dir = path

#     def resolve(self, strict=False):
#         # Resolve based on the custom base directory
#         return self._base_dir / self


class ContentAwarePath(Path, PathToStringMixin, HumanPathPropertiesMixin):
    _flavour = type(Path())._flavour

    @property
    def parent(self) -> "ContentAwarePath":
        return ContentAwarePath(super().parent)

    # @property
    def is_content(self) -> bool:
        """
        Check if the path points to a content inside a file.
        A naive check is to see if the parent exists as a file.
        """
        return self.parent.is_file()

    @property
    def abs(self) -> "ContentAwarePath":
        return ContentAwarePath(self.absolute())

    @property  # fs = filesystem, can return a File or Dir but not content
    def fs(self) -> "ContentAwarePath":
        if self.is_content():
            res = self.parent
        else:
            res = self
        return res

    @property
    def dir(self) -> "ContentAwarePath":
        if self.is_content():
            res = self.parent.parent
        elif self.is_file():
            res = self.parent
        else:
            res = self
        return res

    @property
    def file(self) -> "ContentAwarePath":
        if self.is_content():
            res = self.parent
        elif self.is_file():
            res = self
        else:
            res = None
        return res

    @property
    def ext(self) -> str:
        file = self.file
        if file:
            return file.suffix
        else:
            return ""

class HumanPathPropertiesMixin:
    @property
    def full_path_abs(self) -> str:
        return self.absolute().str

    @property
    def full_path_rel(self) -> str:
        return self.str

    @property
    def file_path_abs(self) -> str:
        return self.file.absolute().str if self.file else "not_file"

    @property
    def file_path_rel(self) -> str:
        return self.file.str if self.file else "not_file"

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

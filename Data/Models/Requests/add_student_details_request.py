from dataclasses import dataclass


@dataclass
class AddStudentDetails:

    first_name: str
    middle_name: str
    last_name: str
    date_of_birth: str

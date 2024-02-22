import pytest
from Data.Clients.book_client import BookClient
from Data.Clients.user_client import UserClient
from Data.Models.Requests.add_student_details_request import AddStudentDetails
from Data.Models.Requests.update_student_details_request import UpdateStudentDetails
from Utils.json_helper import obj_to_json


book_names = [
    ("Git Pocket Guide", "Richard E. Silverman"),
    ("Speaking JavaScript", "Axel Rauschmayer")
]


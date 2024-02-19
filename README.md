# API-automation-framework
API automation framework made with Python, Requests Library, and Pytest.

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Requests](https://img.shields.io/badge/Requests-2CA5E0?style=for-the-badge&logo=python&logoColor=white)


_Installing Libraries from Requirements.txt_ : 

`pip install -r requirements.txt`

**_REPORTING_** :

1) **Python-html Report**

   Install pytest-html command line tool using:

   `pip install pytest-html`

   You are now set to run your test with pytest runner by specifying the directory path to save your HTML report, for example :
   
   `pytest --html=Report.html`
    

   You can open the report that is created in the root directory with the name "Report.html"


2) **Allure Report**

   Install the Allure command line tool using 
   
   `npm install -g allure-commandline --save-dev`


   Steps on how to generate an Allure report using pytest

   -> In your project directory, you first need to generate a folder to save the allure reports, you can automatically generate this with a command:-
   
   `allure generate`
   
   This will create a folder named allure-report in your project directory.
   
   -> You are now set to run your test with pytest runner by specifying the directory path to save your allure report, for example :
   
   `pytest --alluredir=allure-report/`
   
   -> Once test execution completes, all the test results will get stored in the allure-report directory.
   
   You can now view the allure-report in the browser with the command –
   
   `allure serve allure-report/`


  3) **Let's Connect**
     ![LinkedIn](https://img.shields.io/badge/LinkedIn-Profile-blue)](https://www.linkedin.com/in/sayantandsgpta)
     ![Instagram](https://img.shields.io/badge/Instagram-Profile-orange)](https://www.instagram.com/sayan_ta.n/)

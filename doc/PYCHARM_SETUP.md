Check DEVELOPMENT.md to see how to create a Python env (Conda, etc) on your machine and then virtualenv probably to start the development.
  
1- Once Pyhon 3.7 base interpreter (such as Mini Conda) is available on your
  machine, then use it as the 'base interpreter' in the following step:
         
1.1- Go to (Windows) 'Settings' or (Mac) 'Preferences' / 'Project: rheoceros' / 'Project Interpreter' and click on 'Add'.
         
1.2- Choose 'Virtual Env' and create a new Environment by setting your
         new base Python interpreter.

1.3- Verify that root level 'requirements.txt' will be used implicitly for your virtualenv. Check the 'venv' folder and
see if the libraries mentioned in the 'requirements.txt' is auto installed/pip'd by PyCharm.


# Running / Debugging Tests

1- (OPTIONAL) use the interpreter created from the requirements file specific to your test env.

2- Set the unit test tool as 'pytest' from (Windows) 'Settings' or (Mac) 'Preferences' / 'Tools' / 'Python Integrated Tools'

3- Go to the test file. Right click and select either "Run 'pytest in test_example.py' " or "Debug 'pytest in test_example.py' "


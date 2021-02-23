from setuptools import setup, find_packages

setup(
    name="SocialMediaTools",
    version="0.1.2",
    python_requires='>=3.6.0',
    url="https://github.com/AfricasVoices/SocialMediaTools",
    packages=find_packages(exclude=("test",)),
    install_requires=[
        "pytz",
        "requests",
        "python-dateutil",
        "coredatamodules @ git+https://github.com/AfricasVoices/CoreDataModules"
    ]
)

from distutils.core import setup

setup(
    name="general_utils",
    version="1.0",
    description="Multiple classes and function for general use.",
    packages=['general_utils'],
    package_dir={'':'src'},
    author="Juan Galindo",
    author_email="juan.s.galindo@outlook.com",
    install_requires=[
        "aws-lambda-powertools==1.26.0",
        "boto3==1.24.2",
        "requests==2.27.1",
    ],
)

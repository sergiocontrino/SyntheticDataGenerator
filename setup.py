from setuptools import setup, find_packages

setup(
    name='datasynth',
    version='0.0.1',
    url='https://github.com/mypackage.git',
    author='sc',
    author_email='author@gmail.com',
    description='produces synthetic data either by exploring a relational db or using summary statistics from a file',
    packages=find_packages(),
    install_requires=[],
)
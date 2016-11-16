from setuptools import setup, find_packages

setup(
    name='test_attrib_checker',
    version='0.1',
    packages=find_packages(),
    entry_points={
        'nose.plugins': ['test_attrib_checker = '
                         'plugins.test_attrib_checker.test_attrib_checker:TestAttribChecker']
    },
    install_requires=['nose'],
)

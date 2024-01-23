from setuptools import setup, find_packages

setup(
    name='iceberg-diag',
    version='0.1',
    packages=find_packages(),
    install_requires=[
    ],
    entry_points={
        'console_scripts': [
            'iceberg-diag=icebergdiag.cli:main',
        ],
    },
)

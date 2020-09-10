from setuptools import setup, find_packages

with open('libsubmit/version.py') as f:
    exec(f.read())

install_requires = [
    'paramiko',
    'six',
    'configparser',
    'future-fstrings',
    ]

tests_require = [
    'paramiko',
    'six',
    'configparser',
    'future-fstrings',
    'mock>=1.0.0',
    'nose',
    'pytest'
    ]

setup(
    name='libsubmit',
    version=VERSION,
    description='Uniform interface to clouds, clusters, grids and supercomputers.',
    long_description='Submit, track and cancel arbitrary bash scripts on computate resources',
    url='https://github.com/Parsl/libsubmit',
    author='Yadu Nand Babuji',
    author_email='yadu@uchicago.edu',
    license='Apache 2.0',
    download_url = 'https://github.com/Parsl/libsubmit/archive/master.zip',
    package_data={'': ['LICENSE']},
    packages=find_packages(),
    install_requires=install_requires,
    extras_require = {
        'aws' : ['boto3'],
        'azure' : ['azure-mgmt>=2.0.0', 'haikunator'],
        'jetstream' : ['python-novaclient']
        },
    classifiers = [
        # Maturity
        'Development Status :: 3 - Alpha',
        # Intended audience
        'Intended Audience :: Developers',
        # Licence, must match with licence above
        'License :: OSI Approved :: Apache Software License',
        # Python versions supported
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    keywords = ['Workflows', 'Scientific computing'],
)

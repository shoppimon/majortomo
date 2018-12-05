from setuptools import find_packages, setup

with open('VERSION') as f:
    version = f.read()

with open('README.md') as f:
    long_desc = f.read()

setup(
    name='majortomo',
    version=version,
    description='Majortomo - ZMQ MDP 0.2 (Majordomo) Python Implementation',
    author='Shahar Evron',
    author_email='shahar@shoppimon.com',
    url='https://github.com/shoppimon/instellator',
    packages=find_packages(),
    long_description=long_desc,
    long_description_content_type='text/markdown',
    install_requires=[
        "figcan",
        "pyyaml",
        "pyzmq",
        "typing; python_version < '3.0'"
    ],
    test_require=[
        'pytest',
    ],
)

from setuptools import find_packages
from setuptools import setup

REQUIRES = ['aiohttp<=3.4.4']


def readme(fname):
    with open(fname) as fp:
        content = fp.read()
    return content


setup(
    name='aiochclient',
    version='0.0.0',
    description='Async http clickhouse client for python 3.6+',
    long_description=readme('README.md'),
    long_description_content_type="text/markdown",
    author='Danilchenko Maksim',
    author_email='dmax.dev@gmail.com',
    packages=find_packages(exclude=('test*',)),
    package_dir={'aiochclient': 'aiochclient'},
    include_package_data=True,
    install_requires=REQUIRES,
    license='MIT',
    url='https://github.com/maximdanilchenko/aiochclient',
    zip_safe=False,
    keywords='clickhouse async python aiohttp',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    test_suite='tests',
)

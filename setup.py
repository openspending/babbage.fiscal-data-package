from setuptools import setup, find_packages

__version__ = '0.0.1'

setup(
    name='babbage_fiscal',
    version=__version__,
    description="Bi-directional converter babbage/fdp",
    long_description="",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4'
    ],
    keywords='schema jsontableschema jts fdp fiscal data babbage',
    author='OpenSpending',
    author_email='info@openspending.org',
    url='http://github.com/pudo/babbage_fiscal',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'test']),
    namespace_packages=[],
    package_data={},
    include_package_data=True,
    zip_safe=False,
    test_suite='nose.collector',
    install_requires=[
        'babbage >= 0.1.1',
        'normality'
    ],
    tests_require=[
        'nose',
        'coverage',
        'wheel',
        'unicodecsv',
        'jtskit'
    ]
)

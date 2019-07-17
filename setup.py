from setuptools import setup, find_packages

setup(
    name="ODtools",
    version="1.1.1",
    author="zkrPython",
    author_email="178031608@qq.com",
    description="zkrTools",
    long_description=open("README.md",encoding="utf-8").read(),
    license="Apache License",
    url="https://github.com/zkr-origin-data-dpt/ODtools",
    packages=find_packages(),
    include_package_data=True,
    zip_safe=True,
    install_requires=[
        "pykafka"
    ],
    classifiers=[
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Topic :: Text Processing :: Indexing",
        "Topic :: Utilities",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
)

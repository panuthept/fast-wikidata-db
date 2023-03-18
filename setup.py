from setuptools import find_packages, setup


setup(
    name="fast_wikidata_db",
    version="0.0.1",
    description="Fast Wikidata DB",
    author="Panuthep Tasawong",
    author_email="falcon_270394@hotmail.co.th",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
)
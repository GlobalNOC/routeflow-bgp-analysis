from setuptools import setup

setup(name='bgpReport_source',
      version='0.1',
      description='The funniest joke in the world',
      url='http://github.com/storborg/funniest',
      author='Flying Circus',
      author_email='flyingcircus@example.com',
      license='MIT',
      packages=['bgpReport_source'],
      package_data={'bgpReport_source': ['config.json']},
      include_package_data=True,
      install_requires=[
          'simplejson','elasticsearch','mrtparse','wget',
      ],
      zip_safe=False)

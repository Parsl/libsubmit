Libsubmit - Scheduler abstraction
=================================
|licence| |build-status| |docs|

**Libsubmit** provides a uniform interface to submit arbitrary bash scripts to a
variety of execution systems such as clouds, grids, cluster and supercomputers.
This library is designed to simplify submission of pilot systems such as ipython-parallel
to a variety of compute resources.

#The latest version available on PyPi is v0.4.1 .

.. |licence| image:: https://img.shields.io/badge/License-Apache%202.0-blue.svg
   :target: https://github.com/Parsl/libsubmit/blob/master/LICENSE
   :alt: Apache Licence V2.0
.. |build-status| image:: https://travis-ci.org/Parsl/libsubmit.svg?branch=master
   :target: https://travis-ci.org/Parsl/libsubmit
   :alt: Build status
.. |docs| image:: https://readthedocs.org/projects/libsubmit/badge/?version=latest
   :target: http://libsubmit.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status


.. note::
   As of December 20th, (Parsl v0.7.0) the libsubmit repository has been merged into Parsl
   to reduce overheads on maintenance with respect to documentation, testing, and release
   synchronization. The components offered by libsubmit are now available in Parsl as:
   `parsl.channels`, `parsl.launchers` and `parsl.providers`.


Documentation
=============

Developer documentation for libsubmit is available `here <http://libsubmit.readthedocs.io/en/latest/devguide/dev_docs.html#>`_.
Since libsubmit is designed primarily to be used by `Parsl <http://parsl-project.org/>`_ as its resource provider most of the user documentation is blended into Parsl documentation `here <http://parsl.readthedocs.io>`_


.. .. image:: https://readthedocs.org/projects/aws-textract-pipeline/badge/?version=latest
    :target: https://aws-textract-pipeline.readthedocs.io/en/latest/
    :alt: Documentation Status

.. .. image:: https://github.com/MacHu-GWU/aws_textract_pipeline-project/workflows/CI/badge.svg
    :target: https://github.com/MacHu-GWU/aws_textract_pipeline-project/actions?query=workflow:CI

.. image:: https://codecov.io/gh/MacHu-GWU/aws_textract_pipeline-project/branch/main/graph/badge.svg
    :target: https://codecov.io/gh/MacHu-GWU/aws_textract_pipeline-project

.. image:: https://img.shields.io/pypi/v/aws-textract-pipeline.svg
    :target: https://pypi.python.org/pypi/aws-textract-pipeline

.. image:: https://img.shields.io/pypi/l/aws-textract-pipeline.svg
    :target: https://pypi.python.org/pypi/aws-textract-pipeline

.. image:: https://img.shields.io/pypi/pyversions/aws-textract-pipeline.svg
    :target: https://pypi.python.org/pypi/aws-textract-pipeline

.. image:: https://img.shields.io/badge/Release_History!--None.svg?style=social
    :target: https://github.com/MacHu-GWU/aws_textract_pipeline-project/blob/main/release-history.rst

.. image:: https://img.shields.io/badge/STAR_Me_on_GitHub!--None.svg?style=social
    :target: https://github.com/MacHu-GWU/aws_textract_pipeline-project

------

.. .. image:: https://img.shields.io/badge/Link-Document-blue.svg
    :target: https://aws-textract-pipeline.readthedocs.io/en/latest/

.. .. image:: https://img.shields.io/badge/Link-API-blue.svg
    :target: https://aws-textract-pipeline.readthedocs.io/en/latest/py-modindex.html

.. image:: https://img.shields.io/badge/Link-Install-blue.svg
    :target: `install`_

.. image:: https://img.shields.io/badge/Link-GitHub-blue.svg
    :target: https://github.com/MacHu-GWU/aws_textract_pipeline-project

.. image:: https://img.shields.io/badge/Link-Submit_Issue-blue.svg
    :target: https://github.com/MacHu-GWU/aws_textract_pipeline-project/issues

.. image:: https://img.shields.io/badge/Link-Request_Feature-blue.svg
    :target: https://github.com/MacHu-GWU/aws_textract_pipeline-project/issues

.. image:: https://img.shields.io/badge/Link-Download-blue.svg
    :target: https://pypi.org/pypi/aws-textract-pipeline#files


Welcome to ``aws_textract_pipeline`` Documentation
==============================================================================
This project is a low-level implementation of the "Data Store Pipeline" component described in the `Intelligent Document Processing Platform Solution Design <https://dev-exp-share.readthedocs.io/en/latest/search.html?q=Intelligent+Document+Processing+Platform+Solution+Design&check_keywords=yes&area=default>`_ solution.

The term "low-level implementation" implies that this implementation does not rely on AWS services and performs pure in-memory computations. This implementation can be deployed on any platform and is not limited to the AWS ecosystem. It can be deployed as a batch job using virtual machines or containers, or it can be used for real-time processing with an event-driven architecture.


.. _install:

Install
------------------------------------------------------------------------------

``aws_textract_pipeline`` is released on PyPI, so all you need is to:

.. code-block:: console

    $ pip install aws-textract-pipeline

To upgrade to latest version:

.. code-block:: console

    $ pip install --upgrade aws-textract-pipeline

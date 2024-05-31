# TaPS: Task Performance Suite

[![docs](https://github.com/proxystore/taps/actions/workflows/docs.yml/badge.svg)](https://github.com/proxystore/taps/actions)
[![tests](https://github.com/proxystore/taps/actions/workflows/tests.yml/badge.svg)](https://github.com/proxystore/taps/actions)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/proxystore/taps/main.svg)](https://results.pre-commit.ci/latest/github/proxystore/taps/main)

> :warning: **This repository is currently under development.**

TaPS is a standardized framework for evaluating task-based execution frameworks and data management systems using a suite a real and synthetic scientific applications.

TaPS provides:

* A framework for writing benchmark applications and a plugin system for evaluating arbitrary task executors and data management systems.
* A suite of benchmark applications spanning domains like linear algebra, drug discovery, machine learning, text analysis, molecular design, and astronomy.
* Support for popular task execution frameworks ([Dask Distributed](https://distributed.dask.org/), [Globus Compute](https://www.globus.org/compute), [Parsl](https://parsl-project.org/), [Ray](https://www.ray.io/)) and data management systems ([ProxyStore](https://docs.proxystore.dev)).

Check out the [Get Started Guide](https://taps.proxystore.dev/latest/get-started/) to learn more.

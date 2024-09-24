# TaPS: Task Performance Suite

[![docs](https://github.com/proxystore/taps/actions/workflows/docs.yml/badge.svg)](https://github.com/proxystore/taps/actions)
[![tests](https://github.com/proxystore/taps/actions/workflows/tests.yml/badge.svg)](https://github.com/proxystore/taps/actions)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/proxystore/taps/main.svg)](https://results.pre-commit.ci/latest/github/proxystore/taps/main)

TaPS is a standardized framework for evaluating task-based execution frameworks and data management systems using a suite a real and synthetic scientific applications.

TaPS provides:

* A framework for writing benchmark applications and a plugin system for evaluating arbitrary task executors and data management systems.
* A suite of benchmark applications spanning domains like linear algebra, drug discovery, machine learning, text analysis, molecular design, and astronomy.
* Support for popular task execution frameworks ([Dask Distributed](https://distributed.dask.org/), [Globus Compute](https://www.globus.org/compute), [Parsl](https://parsl-project.org/), [Ray](https://www.ray.io/)) and data management systems ([ProxyStore](https://docs.proxystore.dev)).

Check out the [Get Started Guide](https://taps.proxystore.dev/latest/get-started/) to learn more.

## Citation

If you use TaPS or any of this code in your work, please cite our [eScience 2024 Best Paper](https://ieeexplore.ieee.org/document/10678702). The preprint is available on [arXiv](https://arxiv.org/abs/2408.07236).
```bibtex
@inproceedings{pauloski2024taps,
    author = {Pauloski, J. Gregory and Hayot-Sasson, Valerie and Gonthier, Maxime and Hudson, Nathaniel and Pan, Haochen and Zhou, Sicheng and Foster, Ian and Chard, Kyle},
    title = {{TaPS: A Performance Evaluation Suite for Task-based Execution Frameworks}},
    address = {New York, NY, USA},
    booktitle = {IEEE 20th International Conference on e-Science},
    doi = {10.1109/e-Science62913.2024.10678702},
    pages = {1-10},
    publisher = {IEEE},
    year = {2024}
}
```

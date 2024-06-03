# Federated Learning

This submodule implements a *Federated Learning* (FL) application.

At a high level, FL is a paradigm for performing deep learning on decentralized data hosted on decentralized devices (e.g., Internet-of-Things devices).
Each of these devices separately train their own copy of a shared model architecture.
Their locally-trained copies are aggregated over time to update a global model which has essentially been able to learn over all the data in the system without directly accessing any data.

For more information on FL as a whole, please refer to the following [paper](http://proceedings.mlr.press/v54/mcmahan17a/mcmahan17a.pdf).

The `fedlear` application simulates FL with simple deep neural networks and select set of baseline datasets.

## Example

```bash
python -m taps.run fedlearn \
    --executor process-pool \
    --data-name mnist --data-root data/ --data-download true \
    --num-rounds 1 --participation 0.5
```
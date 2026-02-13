# Iceberg Detection via Synthetic Limit Order Book Simulation

## Overview

This project explores whether **iceberg orders** (large hidden institutional orders broken into smaller visible chunks) can be detected using **only observable order book behavior**. Since real exchange-level order book data is expensive and often inaccessible, our approach is to build a **synthetic limit order book simulator**, manually inject iceberg-style execution behavior, and train a model that outputs a **probability that an iceberg is present** at a given price level.

---

## Goal

Build a system that can:

1. **Simulate a realistic limit order book (LOB)** with limit orders, market orders, and cancellations.
2. **Inject iceberg orders manually** (known ground truth).
3. Extract microstructure-inspired features (resilience, refills, volume absorption, etc.).
4. Train a lightweight ML model (logistic regression / random forest / small NN) to output:

> **P(iceberg exists at this price level | recent order book behavior)**

---

## Key Project Idea

We simulate a market where most orders behave normally, but occasionally a large participant enters the market and executes using iceberg-style logic:

* parent order size: large (e.g., 100,000 shares)
* visible tip: small (e.g., 200 shares)
* whenever the tip is filled, it replenishes automatically
* optional randomization in timing and tip size

Because we injected the iceberg ourselves, we have **labels**, enabling model evaluation.

---

## Model Inputs (Features We Track)

* **Price-level resilience** (how much volume is absorbed before price moves)
* **Refill frequency** at a given level
* **Persistence under aggressive flow**
* **Volume traded at a level without depletion**
* **Order imbalance** (bid vs ask pressure)
* **Trade clustering** at a level
* **Queue depletion vs replenishment asymmetry**

---

## Modeling Approach

We start with simple, explainable models:

* Logistic Regression (baseline probability model)
* Decision Tree / Random Forest (nonlinear patterns)
* Optional: small neural network (only if needed)

---

## Evaluation Plan

Since we have ground truth labels (we know where we injected icebergs), we can measure:

* Precision / Recall
* ROC-AUC
* False positive rates under high-noise regimes
* Model performance under different market conditions:

  * high vs low volatility
  * many vs few cancellations
  * multiple icebergs vs none
  * different iceberg refill strategies

---

## Deliverables

By the end of the project, we aim to have:

* A working LOB simulator
* A reproducible dataset generator
* A feature extraction pipeline
* A trained classifier that outputs iceberg probabilities
* Visualizations showing:

  * order book heatmaps over time
  * iceberg vs non-iceberg behavior
  * probability spikes when hidden liquidity is present

---

## Haven't Figured it Out yet

We can pivot to these ideas below if the current project is too hard:

* simplify to tracking only top-of-book instead of full depth
* simulate fewer order types
* switch from ML to purely statistical detection
* treat iceberg detection as anomaly detection instead of classification

---

## Tools / Tech Stack

* Python 3
* NumPy / Pandas
* scikit-learn
* matplotlib
* Jupyter notebooks (for experiments + visualization)

Runs locally on a laptop.

---

## Final Pitch (One Sentence)

**We simulate realistic order books, manually inject iceberg-style hidden liquidity, and train a lightweight ML model to estimate the probability that an iceberg exists based only on observable order book dynamics.**

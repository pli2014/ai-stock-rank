#!/usr/bin/env python3
"""AI Stock Rank - A股趋势分析系统"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="ai-stock-rank",
    version="0.1.0",
    author="AI Stock Team",
    description="基于baostock的A股趋势分析Web服务",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-repo/ai-stock-rank",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Office/Business :: Financial :: Investment",
    ],
    python_requires=">=3.8",
    install_requires=[
        "flask>=2.0.0",
        "baostock>=0.9.1",
        "pandas>=1.3.0",
        "numpy>=1.21.0",
        "flask-gzip>=0.2",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0",
            "black>=21.0",
            "flake8>=3.9",
        ],
    },
    entry_points={
        "console_scripts": [
            "ai-stock-rank=src.main:main",
        ],
    },
)
# Toy DBs: Lightweight Database Implementations

## Description
Toy DBs is a collection of simple, lightweight database implementations designed for educational purposes and to demonstrate various database concepts in a clear and approachable manner.

## Installation
To install Toy DBs, clone the repository and set up a virtual environment:

```bash
git clone https://github.com/tylerwood/toy_dbs.git
cd toy_dbs
python -m venv venv
source venv/bin/activate # On Windows use `venv\Scripts\activate`
```

## Tests
For each implementation there will be a corresponding test in the tests/ directory
Tests can be run using
```python -m unittest tests/{test}.py```


## Usage
Each database in this collection can be used independently. See the README files within each module's directory for specific instructions.

## Modules
- `text_append_only`: An append-only text file based data storage system.
- More modules to be added...

## License
This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments
- Inspired and guided by "Designing Data Intensive Applications" by Martin Kleppmann 


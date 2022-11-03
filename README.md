# tax_loss

## Development

Getting started:

1. conda create -n tax_loss python=3.9 poetry invoke -y # Create conda env
2. conda activate tax_loss # Activate env
3. inv update # Install dependencies
4. inv install # Install package

Running locally:

- poetry run tax_loss

## Building

- inv build

## Testing

- inv test
  Note adding --e2e will include end-to-end tests (which will be slower to run).

## Features

- TODO

Running Frontend in dev

---

1. python -m flask --app "backend/app.py:create_app(<path_to_config_file>)" run --port 5050
2. npm start

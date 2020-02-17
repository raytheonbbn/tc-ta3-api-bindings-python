# Preparation

To prepare this directory, the following was done:
 * Install pip
 * Install sphinx via pip
 * Make this directory manually
 * Within this directory, run:
    ```sh
    sphinx-quickstart
    ```
 * Within this directory, run:
    ```sh
    sphinx-apidoc ../tc -o .
    ```

# Making documentation
To manually make documentation within this directory, run:
    ```sh
    make html
    ```

This should place documentation under the `_build/html` directory.

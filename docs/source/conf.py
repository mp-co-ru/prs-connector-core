# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'prs-connector-core'
copyright = '2025, ООО МПК'
author = 'ООО МПК'
release = '0.8.0'
version = '0.8.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinxcontrib.httpdomain',
    'sphinxcontrib.httpexample',
    'sphinx.ext.todo',
    'sphinx_togglebutton'
]

templates_path = ['_templates']
exclude_patterns = []

language = 'ru'

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
html_favicon = "_static/peresvet_middle.png"
html_logo = "pics/logo.png"
#html_theme_options = {
#    'logo_only': True,
#    'display_version': True,
#}
togglebutton_hint = ""
togglebutton_hint_hide = ""

html_theme_options = {
    'logo_only': True,
    'display_version': True,
}
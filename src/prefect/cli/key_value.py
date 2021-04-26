import click
from click.exceptions import Abort
from tabulate import tabulate

from prefect import Client, config
from prefect.utilities.exceptions import AuthorizationError, ClientError


@click.group(hidden=True)
def kv():
    """
    Handle Prefect Cloud authorization.

    \b
    Usage:
        $ prefect kv [COMMAND]

    \b
    Arguments:
        set             Set a key value pair
        get             Get the value associated with a key
        delete          Delete a key value pair
        list            List key value pairs
        import-json     Set key value pairs from json file

    \b
    Examples:
        $ prefect kv set my_key my_val

    \b
        $ prefect kv get my_key
        my_val
    """
    if config.backend == "server":
        raise click.UsageError(
            "Key value commands with server are not currently supported."
        )


@kv.command(name="set", hidden=True)
@click.argument("key")
@click.argument("value")
def _set(key, value):
    """
    Set a key value pair, overriding existing values if key exists

    \b
    Arguments:
        key         TEXT    Key to set
        value       TEXT    Value associated with key to set
    """
    client = Client()
    result = client.set_key_value_pairs(names=[key], values=[value])

    if result is not None:
        click.secho("Key set successfully", fg="green")
    else:
        click.echo("An error occurred setting the key value pair", fg="red")


@kv.command(hidden=True)
@click.argument("key")
def get(key):
    """
    Get the value of a key

    \b
    Arguments:
        key         TEXT    Key to get
    """
    client = Client()
    result = client.get_key_value(name=key)
    if result is not None:
        click.secho(f"Key {key} has value {result}", fg="green")
    else:
        click.echo("An error occurred getting the key", fg="red")


@kv.command(hidden=True)
@click.argument("key")
def delete(key):
    """
    Get the the key value pair

    \b
    Arguments:
        key         TEXT    Key to delete
    """
    client = Client()
    result = client.delete_key_value(name=key)
    if result:
        click.secho(f"Key {key} has been deleted", fg="green")
    else:
        click.echo("An error occurred deleting the key", fg="red")


@kv.command(name="list", hidden=True)
def _list():
    """
    List all key value pairs
    """
    client = Client()
    result = client.list_key_values()

    click.echo(
        tabulate(
            result,
            headers=["NAME", "VALUE"],
            tablefmt="plain",
            numalign="left",
            stralign="left",
        )
    )


@kv.command(hidden=True)
@click.argument("path")
def import_json(path):
    """
    Set key value pairs based on json
    """
    import json

    with open(path, "rb") as fp:
        json_info = json.load(fp)

    client = Client()
    result = client.set_key_value_pairs_from_nested_dict(key_value_dict=json_info)

    click.echo(result)

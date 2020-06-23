.. _install:

Introduction
============

`aiochclient` is an async http(s) ClickHouse client for python 3.6+ supporting
type conversion in both directions, streaming, lazy decoding on select queries,
and a fully typed interface.

Use `aiochclient` for a simple interface into your ClickHouse deployment.

Requirements
------------

`aiochclient` works on Linux, OSX, and Windows.

It requires Python >= 3.6 due to the use of types.

Installation
------------

You can install `aiochclient` with `pip` or your favourite package manager:

::

    $ pip install aiochclient


Add the ``-U`` switch to update to the latest version if `aiochclient` is already
installed.

Quick Start
-----------

The quickest way to get up and running with `aiochclient` is to simply connect
and check ClickHouse is alive. Here's how you would do that:

::

    import asyncio
    from aiochclient import ChClient
    from aiohttp import ClientSession

    async def main():
        async with ClientSession() as s:
            client = ChClient(s)
            alive = await client.is_alive()
            print(f"Is ClickHouse alive? -> {alive}")

    if __name__ == '__main__':
        asyncio.run(main())

This automatically queries a instance of ClickHouse on `localhost:8123` with the
default user. You may want to set up a different connection to test. To do that,
change the following line::
    client = ChClient(s)

To something like::

    client = ChClient(s, url='http://localhost:8123')

You can find more options for connecting to ClickHouse in the :ref:`api`.
Continue reading to learn more about `aiochclient`.

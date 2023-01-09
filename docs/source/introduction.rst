.. _install:

Introduction
============

`myscaledb-client` is an async/sync http(s) Clickhouse client for python 3.6+ supporting
type conversion in both directions, streaming, lazy decoding on select queries,
and a fully typed interface.

MyscaleDB is a modified database based on Clickhouse, so we redeveloped `aiochclient` to make it fit our new database, and provides sync client to connect Clickhouse.

Use `myscaledb-client` for a simple interface into your Clickhouse deployment.

Requirements
------------

`myscaledb-client` works on Linux, OSX, and Windows.

It requires Python >= 3.6 due to the use of types.

Installation
------------

You can install `myscaledb-client` with `pip` or your favourite package manager:

::

    $ pip install myscaledb-client


Add the ``-U`` switch to update to the latest version if `myscaledb-client` is already
installed.

Quick Start
-----------

The quickest way to get up and running with `myscaledb-client` is to simply connect
and check Clickhouse is alive. Here's how you would do that:

::

    # This is a demo using AsyncClient.
    # AsyncClient can give you a higher degree of concurrency, it requires an understanding of asynchronous programming.

    import asyncio
    from myscaledb import AsyncClient
    from aiohttp import ClientSession

    async def main():
        async with ClientSession() as s:
            async with AsyncClient(s) as client:
                alive = await client.is_alive()
                print(f"Is Clickhouse alive? -> {alive}")

    if __name__ == '__main__':
        asyncio.run(main())

::

    # This is a demo using Client.
    # Client works in sync mode, in line with most people's programming habits.

    from myscaledb import Client

    def main():
        client = Client()
        alive = client.is_alive()
        print(f"Is Clickhouse alive? -> {alive}")

    if __name__ == '__main__':
        main()

This automatically queries a instance of Clickhouse on `localhost:8123` with the
default user. You may want to set up a different connection to test. To do that,
change the following line::
    client = Client()

To something like::

    client = Client(url='http://localhost:8123')

You can find more sample code to operate Clickhouse in the :ref:`API Reference`.

Continue reading to learn more about `myscaledb-client`.

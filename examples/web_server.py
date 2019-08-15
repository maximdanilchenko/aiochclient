from aiohttp import ClientSession, web

from aiochclient import ChClient

###
# Signals to init and close http connections pool
###


async def init_ch_client(app: web.Application):
    app['http_session'] = ClientSession()
    app['ch'] = ChClient(app['http_session'], **app['config']['clickhouse'])


async def close_ch_client(app: web.Application):
    await app['http_session'].close()


###
# Simple handler to show how to use chclient in aiohttp app
###


async def handler(request: web.Request) -> web.Response:
    ch: ChClient = request.app['ch']
    is_alive = await ch.is_alive()
    return web.json_response({'is_alive': is_alive})


###
# Aiohttp app factory
###


def create_app(config):
    app = web.Application()
    app['config'] = config

    app.on_startup.append(init_ch_client)
    app.on_cleanup.append(close_ch_client)

    app.router.add_get('/ch_alive', handler)
    return app


if __name__ == '__main__':
    # Config can be loaded from env or some config files
    APP_CONFIG = {
        'clickhouse': {
            'url': 'https://localhost:8123',
            'user': 'user',
            'password': 'password',
            'database': 'db',
            'compress_response': True,
        }
    }
    # Create app
    app = create_app(APP_CONFIG)
    # Run app
    web.run_app(app)

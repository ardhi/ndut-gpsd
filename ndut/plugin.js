const Gpsd = require('node-gpsd-client')
const clients = {}

const connect = function (name, options) {
  const { _, getNdutConfig } = this.ndut.helper
  const queueCfg = getNdutConfig('ndutQueue')
  const { Queue } = this.ndutQueue.helper.bullmq
  const conn = options.connection[name]
  if (!conn) this.log.error(`No GPSD connection named '${name}'`)
  else {
    const c = new Gpsd(_.omit(conn, ['autoConnect']))
    if (conn.watch) c.watch(conn.watch)
    c.on('connected', val => {
      this.log.debug(`[GPSD] Connection '${name}' connected`)
    })
    c.on('disconnected', val => {
      this.log.debug(`[GPSD] Connection '${name}' disconnected`)
    })
    c.on('reconnecting', val => {
      this.log.debug(`[GPSD] Connection '${name}' reconnecting`)
    })
    c.on('error', err => {
      this.log.error(`[GPSD] Error: ${err.message}`)
    })
    if (conn.watch) {
      const queue = new Queue('gpsd', { connection: queueCfg.connection })
      c.connect()
      c.unwatch()
      const json = _.merge({}, { class: 'WATCH', json: true }, conn.watch.options || {})
      c.watch(json)
      if (conn.watch.event) {
        c.removeAllListeners(conn.watch.event)
        c.on(conn.watch.event, async data => {
          const source = conn.deviceMap ? conn.deviceMap[data.device] : data.device
          const siteId = conn.siteId || 'DEFAULT'
          queue.add('AIS', { source, data, siteId }, { removeOnComplete: true }).then()
        })
      }
    }
    clients[name] = c
  }
}

const plugin = async function (scope, options) {
  const { _ } = scope.ndut.helper
  _.forOwn(options.connection, (v, k) => {
    connect.call(scope, k, options)
  })
  scope.ndutGpsd.clients = clients
}

module.exports = async function () {
  const { fp } = this.ndut.helper
  return fp(plugin)
}

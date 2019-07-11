const { Graph } = require('./index')

async function main() {
  const g = new Graph('demo')
  const v1 = await g.addVertex({name: 'Tom Hanks'})
  const v2 = await g.addVertex({name: 'Forest Gump'})
  const e = await g.addEdge(v1, v2, 'actedIn')

  const actors = (await g.v()
    .has({name: 'Forest Gump'})
    .inE('actedIn')
    .outV()
    //.in('actedIn')
    .toArray())
    .map(v => v.name)

  //g.level.forEach(console.log)
  console.log(actors)
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
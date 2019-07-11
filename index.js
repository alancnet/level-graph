const level = require('level-sugar')
const { from } = require('rxjs')
const { filter, map, flatMap, flat, toArray, distinct } = require('rxjs/operators')

const _ = require('lodash')
const uuid = require('uuid')
const RID = '@rid'
const TYPE = '@type'
const VERTEX = 'VERTEX'
const EDGE = 'EDGE'

const isTraversal = (t) => t && t instanceof Traversal
function assertIsTraversal (t) {
  if (!isTraversal(t)) throw new Error('Expected Traversal')
}
const isVertex = (v) => v && v[TYPE] === VERTEX
function assertIsVertex (v) {
  if (!isVertex(v)) throw new Error('Expected Vertex')
}
const isEdge = (e) => e && e[TYPE] === EDGE
function assertIsEdge (e) {
  if (!isEdge(e)) throw new Error('Expected Edge')
}
const isLabel = (s) => s && typeof s === 'string'
function assertIsLabel (s) {
  if (!isLabel(s)) throw new Error('Expected non-empty string as Label')
}
const isNode = (n) => n && n[TYPE] === VERTEX || n[TYPE] === EDGE
function assertIsNode (s) {
  if (!isNode(s)) throw new Error('Expected Vertex or Edge')
}
const flatten = (array) => {
  const flatter = array.reduce((a, b) => a.concat(b), [])
  if (flatter.length > array.length) return flatten(flatter)
  else return flatter
}
const filterArray = (array, predicate) => {
  var count = 0
  for (var i = 0; i < array.length; i++) {
    if (predicate(array[i])) {
      array.splice(i--, 1)
      count++
    }
  }
  return count
}
const merge = (parent, traversals) => new Traversal(parent.graph, parent,
  flatten(traversals)
  .map(x => isTraversal(x) ? x.nodes : x(parent).nodes)
  .reduce((a, b) => a.concat(b), [])
)
const singleVertex = (v) => {
  if (isTraversal(v)) return singleVertex(v.nodes)
  if (isVertex(v)) return v
  if (Array.isArray(v)) {
    if (v.length !== 1) throw new Error(`Expected 1 Vertex. Got ${v.length}.`)
    return singleVertex(v[0])
  }
  throw new Error('Expected Vertex, Traversal, or Array')
}
const singleEdge = (v) => {
  if (isTraversal(v)) return singleEdge(v.nodes)
  if (isEdge(v)) return v
  if (Array.isArray(v)) {
    if (v.length !== 1) throw new Error(`Expected 1 Edge. Got ${v.length}.`)
    return singleEdge(v[0])
  }
  throw new Error('Expected Edge, Traversal, or Array')
}

const outKey = (label) => `out_${label}`
const inKey = (label) => `in_${label}`
const identity = x => x
const truthy = x => !!x

class Graph {
  constructor (...levelArgs) {
    this.level = level(...levelArgs)
    // this.vertices = data && data.vertices || []
    // this.edges = data && data.edges || []
    // this.nodes = {}
    // this.vertices.forEach(v => this.register(v))
    // this.edges.forEach(e => this.register(e))
  }
  async register (node) {
    assertIsNode(node)
    if (isVertex(node)) this.level.vertices.put(node[RID], node)
    else if (isEdge(node)) this.level.edges.put(node[RID], node)
    else throw new Error(`Unexpected type: ${node[TYPE]}`)
  }
  async addVertex (obj) {
    const vertex = Object.assign({[RID]: uuid(), [TYPE]: VERTEX}, obj)
    await this.register(vertex)
    return new Traversal(this, this.v, [vertex])
  }
  async addEdge (outV, inV, label, obj) {
    outV = await singleVertex(outV)
    inV = await singleVertex(inV)
    assertIsLabel(label)
    const edge = Object.assign(
      {[RID]: uuid(), [TYPE]: EDGE},
      obj,
      {
        out: outV[RID],
        in: inV[RID],
        label
      }
    )
    await this.register(edge)
    const ok = outKey(label)
    const ik = inKey(label)
    if (!outV[ok]) outV[ok] = []
    if (!inV[ik]) inV[ik] = []
    outV[ok].push(edge[RID])
    inV[ik].push(edge[RID])
    await this.register(outV)
    await this.register(inV)
    return new Traversal(this, this.v, [edge])
  }
  async remove (node) {
    // if () await this.level.vertices.del(node[RID])
    // else if (isEdge(node)) await this.level.edges.del(node[RID])
    if (isVertex(node)) {
      for (let key in node) {
        if (key.startsWith('out_')) {
          for (let fk of node[key]) {
            const edge = await this.level.edges.get(fk)
            const vertex = await this.vertices.get(edge.in)
            filterArray(vertex[inKey(edge.label)], k => k === node[RID])
            await this.level.edges.del(edge[RID])
            await this.register(vertex)
          }
        }
        if (key.startsWith('in_')) {
          for (let fk of node[key]) {
            const edge = await this.level.edges.get(fk)
            const vertex = await this.vertices.get(edge.out)
            filterArray(vertex[inKey(edge.label)], k => k === node[RID])
            await this.level.edges.del(edge[RID])
            await this.register(vertex)
          }
        }
      }
      await this.level.vertices.del(node[RID])
    } else if (node[TYPE] === EDGE) {
      const outV = await this.level.vertices.get(node.out)
      const inV = await this.level.vertices.get(node.in)
      const ok = outKey(node.label)
      const ik = inKey(node.label)
      filterArray(outV[ok], k => k === node[RID])
      filterArray(inV[ik], k => k === node[RID])
      filterArray(this.edges, e => e === node)
      await this.register(outV)
      await this.register(inV)
      await this.edges.del(node[RID])
    }
  }
  v (rid) {
    if (rid && rid[RID]) rid = rid[RID]
    if (rid) {
      const rids = Array.isArray(rid) ? rid : [rid]
      return new Traversal(this, null, from(rids).pipe(distinct(), filter(identity), flatMap(rid => this.level.vertices.get(rid)), filter(identity)))
    } else {
      return new Traversal(this, null, this.level.vertices.stream.pipe(map(x => x.value)))
    }
  }
  e (rid) {
    if (rid && rid[RID]) rid = rid[RID]
    if (rid) {
      const rids = Array.isArray(rid) ? rid : [rid]
      return new Traversal(this, null, from(rids).pipe(distinct(), filter(identity), flatMap(rid => this.level.edges.get(rid)), filter(identity)))
    } else {
      return new Traversal(this, null, this.level.edges.stream.pipe(map(x => x.value)))
    }
  }
  // manifest (VertexClass, EdgeClass) {
  //   if (!VertexClass) VertexClass = Object
  //   if (!EdgeClass) EdgeClass = Object
  //   const nodes = _.cloneDeep(this.nodes)
  //   this.vertices.map(v => v[RID])
  //     .forEach(rid => (
  //       nodes[rid] = Object.assign(new VertexClass(nodes[rid]), nodes[rid])
  //     ))
  //   this.edges.map(v => v[RID])
  //     .forEach(rid => (
  //       nodes[rid] = Object.assign(new EdgeClass(nodes[rid]), nodes[rid])
  //     ))
  //   const vertices = this.vertices.map(v => nodes[v[RID]])
  //   const edges = this.edges.map(e => nodes[e[RID]])
  //   vertices.forEach(v => Object.keys(v).forEach(key => {
  //     if (key.startsWith('out_') || key.startsWith('in_')) {
  //       const arr = v[key]
  //       arr.forEach((x, i) => {
  //         arr[i] = nodes[x]
  //       })
  //     }
  //   }))
  //   edges.forEach(e => {
  //     e.out = nodes[e.out]
  //     e.in = nodes[e.in]
  //   })
  //   return {vertices, edges}
  // }
  // export () {
  //   return {
  //     vertices: _.cloneDeep(this.vertices),
  //     edges: _.cloneDeep(this.edges)
  //   }
  // }
}

class Traversal {
  constructor (graph, parent, nodes) {
    this.parent = parent
    this.graph = graph
    this.nodes = nodes
    this.name = null
  }
  _next (...operators) {
    return new Traversal(this.graph, this, this.nodes.pipe(...operators))
  }
  addEdge (label, other, obj) {
    assertIsTraversal(other)
    return merge(
      this,
      this.nodes.map(outV =>
        other.nodes.map(inV =>
          this.graph.addEdge(outV, inV, label, obj)
        )
      )
    )
  }
  has (obj) {
    const matcher = _.matches(obj)
    return this._next(filter(matcher))
  }
  hasNot (obj) {
    const matcher = _.matches(obj)
    return this._next(filter(n => !matcher(n)))
  }
  outE (label) {
    const key = outKey(label)
    return this._next(
      flatMap(n => n[key]),
      toArray(),
      flatMap(rids => this.graph.e(rids).toArray()),
      flatMap(identity)
    )
  }
  inE (label) {
    const key = inKey(label)
    return this._next(
      flatMap(n => n[key]),
      toArray(),
      flatMap(rids => this.graph.e(rids).toArray()),
      flatMap(identity)
    )
  }
  outV () {
    return this._next(
      map(n => n.out),
      toArray(),
      flatMap(rids => this.graph.v(rids).toArray()),
      flatMap(identity)
    )
  }
  inV () {
    return this._next(
      map(n => n.in),
      toArray(),
      flatMap(rids => this.graph.v(rids).toArray()),
      flatMap(identity)
    )
  }
  out (label) {
    return this.outE(label).inV()
  }
  in (label) {
    return this.inE(label).outV()
  }
  both (label) {
    return merge(this, [
      this.out(label),
      this.in(label)
    ])
  }
  bothV (label) {
    return merge(this, [
      this.outV(label),
      this.inV(label)
    ])
  }
  bothE (label) {
    return merge(this, [
      this.outE(label),
      this.inE(label)
    ])
  }
  dedup () {
    var rids = {}
    return this._next(
      this.nodes.filter((n) =>
        !rids[n[RID]] && (rids[n[RID]] = n)
      )
    )
  }
  or (...conditions) {
    return this._boolean((a, b) => a || b, false, conditions)
  }
  and (...conditions) {
    return this._boolean((a, b) => a && b, true, conditions)
  }
  except (...conditions) {
    return this._boolean((a, b) => a && !b, true, conditions)
  }
  retain (conditions) {
    return this.and(merge(this, conditions))
  }
  _boolean (operator, initial, conditions) {
    const results = conditions
      .map(merge)
      .map(x =>
        x.nodes
          .reduce((set, n) => {
            set[n[RID]] = n
            return set
          }, {})
      )
    const reduction = this.nodes.filter(n =>
      results.reduce((cond, set) => operator(cond, !!set[n[RID]]), initial)
    )
    return this._next(reduction)
  }
  as (name) {
    return Object.assign(this._next(), {name})
  }
  back (name) {
    return this.name === name ? this
      : this.parent && this.parent.back(name)
  }
  filter (predicate) {
    return this._next(this.nodes.filter(predicate))
  }
  interval (prop, lower, upper) {
    return this._next(this.nodes.filter(n => n[prop] >= lower && n[prop] < upper))
  }
  random (bias) {
    return this._next(this.nodes.filter(n => Math.random() < bias))
  }
  map (fields) {
    fields = flatten(Array.from(arguments)).concat(RID, TYPE)
    return this._next(this.nodes.map(n => _.pick(n, fields)))
  }
  select (names) {
    const named = {}
    const collect = (t) => {
      if (t) {
        collect(t.parent)
        if (t.name) named[t.name] = t.nodes
      }
    }
    collect()
    return this._next([named])
  }
  toArray () {
    return new Promise((resolve, reject) => {
      this.nodes.pipe(
        toArray()
      ).subscribe(resolve, reject, reject)
    })
  }
  first () {
    return this.nodes[0] || null
  }
  remove () {
    this.nodes.forEach(n => this.graph.remove(n))
    return this._next([])
  }
}

module.exports = {
  RID,
  TYPE,
  VERTEX,
  EDGE,
  isTraversal,
  assertIsTraversal,
  isVertex,
  assertIsVertex,
  isEdge,
  assertIsEdge,
  isLabel,
  assertIsLabel,
  isNode,
  assertIsNode,
  flatten,
  merge,
  outKey,
  inKey,
  identity,
  truthy,
  Graph,
  Traversal
}

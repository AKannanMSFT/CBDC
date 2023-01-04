# Databricks notebook source
from enum import Enum
import random as random

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enumerator of Node Type
# MAGIC  - GSCC is the densely connected component in the middle
# MAGIC  - GOUT only has inward links (outward from GSCC)
# MAGIC  - GIN only has outward links (inward into GSCC)
# MAGIC  - DC are disconnected components on the side
# MAGIC  
# MAGIC  ![Node Type](https://www.researchgate.net/profile/Joel-Miller-9/publication/50851337/figure/fig3/AS:317399011217414@1452685229154/Schematic-diagram-of-the-giant-components-of-an-EPN-Note-that-the-GIN-and-GOUT-both.png)

# COMMAND ----------

class NodeType(Enum):
  GSCC = 'GSCC'
  GOUT = 'GOUT'
  GIN = 'GIN'
  DC = 'DC'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Node Sampler
# MAGIC Class to sample nodes in accordance with a predefined sampling rate (depending on the node type)

# COMMAND ----------

class NodeSampler:
  samplingrate = {
    NodeType.GSCC:0.78,
    NodeType.GOUT:0.12,
    NodeType.GIN:0.08,
    NodeType.DC:0.02    
  }
  
  @staticmethod
  def sampleNode():
    rvar = random.random()
    rtotal = 0
    for k in NodeSampler.samplingrate:
      rtotal = rtotal + NodeSampler.samplingrate[k]
      if rtotal >= rvar:
        return k

# COMMAND ----------

# MAGIC %md
# MAGIC ### Node
# MAGIC Data structure to save the node

# COMMAND ----------

class Node:
  def __init__(self, Name):
    self.Type = NodeSampler.sampleNode()
    self.Name = Name
  
  def getName(self):
    return self.Name
  
  def getType(self):
    return self.Type
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Edge
# MAGIC Data structure to save the edge. One cannot construct an edge that does not abide by the node type constraints

# COMMAND ----------

class Edge:
  def __init__(self,Source,Dest):
    
    if Source.Type == NodeType.GOUT or Dest.Type == NodeType.GIN:
      raise TypeError("Direction constraint")
    
    if (Source.Type == NodeType.DC and not Dest.Type == NodeType.DC) or (Dest.Type == NodeType.DC and not Source.Type == NodeType.DC):
      raise TypeError("Disconnected Component constraint")          
    
    self.Source = Source
    self.Dest = Dest
    self.Reciproc = False
    
    if Source.Type == NodeType.GSCC and Dest.Type == NodeType.GSCC:
      self.Reciproc = True
  
  def getKey(self):
    return "{}->{}".format(self.Source.getName(),self.Dest.getName())
  
  def getSource(self):
    return self.Source
  
  def getDest(self):
    return self.Dest
  
  def getReciproc(self):
    return self.Reciproc
  
  def constructReciproc(self):
    return Edge(self.Dest,self.Source)
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Edge Sampler
# MAGIC Class to sample edges in accordance with predefined connectivity and reciprocity
# MAGIC 
# MAGIC  - Automatically samples nodes
# MAGIC  - Invalid edges will not be sampled
# MAGIC  - Reciprocal edges are created from existing GSCC only edges

# COMMAND ----------

class EdgeSampler:
  def __init__(self,node_count,connectivity,reciprocity,verbose=False):
    self.sampled_nodes = [Node('{}'.format(x)) for x in range(node_count)]
    self.num_edges = int(connectivity * node_count * (node_count-1))
    self.reciprocity = reciprocity
    self.edges = []
    self.verbose = verbose
  
  def getNodes(self):
    return self.sampled_nodes
  
  def sampleEdge(self):
    edge = None
    noSampleYet = True
    while noSampleYet:
      edgeSample = random.sample(self.sampled_nodes, 2)
      try:
        edge = Edge(edgeSample[0],edgeSample[1])
        noSampleYet = False
      except TypeError as terr:
        if self.verbose:
          print("Resampling due to", terr)
    
    return edge
  
  def sampleEdges(self):
    edges = set()
    reciproc = []
    
    num_edge = 0
    while num_edge < self.num_edges:
      
      if random.random() > self.reciprocity or len(reciproc)==0:
        edge = self.sampleEdge()
      else:
        edge = random.sample(reciproc,1)[0].constructReciproc()
        
      if not edge.getKey() in edges:
        num_edge = num_edge + 1
        self.edges.append(edge)
        edges.add(edge.getKey())
        
        if edge.getReciproc():
          reciproc.append(edge)
    
    return self.edges

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interesting Stats
# MAGIC Class to compute interesting stats about the network. Useful to make sure that sampling is done right

# COMMAND ----------

class InterstingStats:  
  @staticmethod
  def getReciprocityRate(samples):
    keySet = set()
    for x in samples:
      keySet.add(x.getKey())

    reciprocalEdge = 0
    for x in samples:
      reciprocalKey = "{}->{}".format(x.getDest().getName(),x.getSource().getName())
      if reciprocalKey in keySet:
        reciprocalEdge = reciprocalEdge + 1

    print('{:.2f}%'.format(reciprocalEdge*100.0/len(keySet)))
  
  @staticmethod
  def getNodeTypeDistribution(samples):
    ntdic = {}
    slen = len(samples)
    for sample in samples:
      ntype = sample.getType()
      nname = sample.getName()
      if not ntype in ntdic:
        ntdic[ntype] = 0
      ntdic[ntype] += 1
    
    retStrFrmt = '{}:{:.2f}%\n'
    retStr = ''
    for nt in ntdic:
      ntdic[nt] = ntdic[nt] * 100.0/slen
      retStr += retStrFrmt.format(nt.value,ntdic[nt])
    
    print(retStr)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution Section

# COMMAND ----------

es = EdgeSampler(7000,0.003,0.15)
edgesample = es.sampleEdges()

# COMMAND ----------

InterstingStats.getReciprocityRate(edgesample)

# COMMAND ----------

InterstingStats.getNodeTypeDistribution(es.getNodes())
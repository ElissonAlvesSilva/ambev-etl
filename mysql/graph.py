#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pygraph.classes.digraph import digraph
from pygraph.readwrite.dot import write

from config import Config
from utils.log import Log


class Graph:
	def __init__(self, jobs_to_run):
		self.filename = "graph.pdf"
		self.jobs_to_run = jobs_to_run
		self.graph = None
		self._create_graph()

	def _create_graph(self):
		Log.Instance().appendFinalReport("Generating graph for jobs " + str(self.jobs_to_run))
		dag = digraph()
		for job in self.jobs_to_run:
			self._add_node(dag, job.name,
										 attrs=[("shape", "none"), ("label", self._tabulated_label(job.name))])
			if len(job.previous_jobs) > 0:
				self._add_edge(dag, job.name, job.previous_jobs)
		for final_node in [node for node in dag.nodes() if not dag.neighbors(node)]:
			self._set_node_attr(dag, final_node, [("color", "green")])
		for start_node in [node for node in dag.nodes() if not dag.incidents(node)]:
			self._set_node_attr(dag, start_node, [("color", "blue")])
		self.graph = dag
		Log.Instance().appendFinalReport("Execution graph generated, a PDF was saved at " +
																		 self.filename + "\n")

	def _add_node(self, graph, node, attrs=[]):
		try:
			graph.add_node(node, attrs=attrs)
		except:
			self._set_node_attr(graph, node, attrs)

	def _set_node_attr(self, graph, node, attrs):
		if graph.node_attr.get(node):
			for key, value in attrs:
				if key not in [x[0] for x in graph.node_attr[node]]:
					graph.node_attr[node].append((key, value))
		else:
			graph.node_attr[node] = attrs

	def _add_edge(self, graph, node, edges):
		for edge_node in edges:
			if (Config.PARTIAL_RUN and edge_node in Config.JOBS_NAMES) or not Config.PARTIAL_RUN:
				if edge_node not in graph.nodes():
					graph.add_node(edge_node)
				graph.add_edge((edge_node, node))

	def _tabulated_label(self, job_name):
		return '<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0"><TR><TD ' \
					 'COLSPAN="2" BGCOLOR="lightgrey">' + job_name + '</TD></TR></TABLE>>'

	def previous_jobs(self, job):
		node = list(filter(lambda item: item == job.name, self.graph.nodes()))[0]
		return self.graph.incidents(node)

	def next_jobs(self, job):
		node = list(filter(lambda item: item == job.name, self.graph.nodes()))[0]
		return self.graph.neighbors(node)

import pandas as pd
import networkx as nx
import operator
import os
os.getcwd()
# your current working directory will be displayed
os.path.exists('edges.csv')
os.path.exists('nodes.csv')
# Must be True, otherwise it is unrelated to pandas

def add_link(a,b, net):
	net.add_node(a, group = 'edge', color = 'red')
	net.add_node(b, group = 'node', color = 'blue')
	net.add_edge(a, b)
def main():

	net =nx.DiGraph()

	# Read Data
	data_types = {'edge': 'int32', 'node': 'int32'}
	edge = pd.read_csv('edges.csv', dtype = data_types)
	n = len(edge.index)

	# Read nodes  for print out
	node =  pd.read_csv('nodes.csv', dtype = data_types)
	node.set_index('node', inplace = True)
	nod_dict = node.to_dict(orient='index')

	# Loop through edge lables and add edges
	for row in edge.itertuples():
		index = row[0]

		edge = 'u' + str(row[1])
		node = 'm' + str(row[2])
		#node = row[3]
		add_link(edge, node, net)
		print("Added Edge {0} of {1}".format(index + 1, n))

	# Print node count
	print("Total Amount of Nodes: {}".format(len(net)/2))

	# Run HITS algorithim
	hubs, auth = nx.hits(net, max_iter = 1000)
	sorted_auth = sorted(auth.items(), key=operator.itemgetter(1), reverse = True)

        def hits(G,max_iter=100,tol=1.0e-8,nstart=None,normalized=True):
        if type(G) == nx.MultiGraph or type(G) == nx.MultiDiGraph:
        raise Exception("hits() not defined for graphs with multiedges.")
        if len(G) == 0:
        return {},{}
        # choose fixed starting vector if not given
                if nstart is None:
                h=dict.fromkeys(G,1.0/G.number_of_nodes())
                else:
                h=nstart
        # normalize starting vector
                s=1.0/sum(h.values())
                for k in h:
                h[k]*=s
                i=0
        while True: # power iteration: make up to max_iter iterations
                hlast=h
                h=dict.fromkeys(hlast.keys(),0)
                a=dict.fromkeys(hlast.keys(),0)
        # this "matrix multiply" looks odd because it is
        # doing a left multiply a^T=hlast^T*G
        for n in h:
            for nbr in G[n]:
                a[nbr]+=hlast[n]*G[n][nbr].get('weight',1)
        # now multiply h=Ga
        for n in h:
            for nbr in G[n]:
                h[n]+=a[nbr]*G[n][nbr].get('weight',1)
        # normalize vector
        s=1.0/max(h.values())
        for n in h: h[n]*=s
        # normalize vector
        s=1.0/max(a.values())
        for n in a: a[n]*=s
        # check convergence, l1 norm
        err=sum([abs(h[n]-hlast[n]) for n in h])
        if err < tol:
            break
        if i>max_iter:
            raise NetworkXError(\
            "HITS: power iteration failed to converge in %d iterations."%(i+1))
        i+=1
    if normalized:
        s = 1.0/sum(a.values())
        for n in a:
            a[n] *= s
        s = 1.0/sum(h.values())
        for n in h:
            h[n] *= s
    return h,a

def authority_matrix(G,nodelist=None):
    """Return the HITS authority matrix."""
    M=nx.to_numpy_matrix(G,nodelist=nodelist)
    return M.T*M

def hub_matrix(G,nodelist=None):
    """Return the HITS hub matrix."""
    M=nx.to_numpy_matrix(G,nodelist=nodelist)
    return M*M.T
                                  
	# Print top 1133
	for i in range(1133):
		nod = int(sorted_auth[i][0].replace("m", ""))
		print("{0}. {1}, {2}\n".format(i+1, nod_dict[nod]['dummy'], sorted_auth[i][1]))

main()



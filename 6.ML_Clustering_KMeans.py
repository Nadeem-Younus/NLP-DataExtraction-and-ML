# --------------- KMeans -------------------#
# --------------- KMeans Elbow -------------#

from sklearn.cluster import KMeans
true_k = 5
model = KMeans(n_clusters=true_k, init='k-means++', max_iter=100, n_init=1)
model.fit(X_tfidf)


print("Top terms per cluster :-\n")
order_centroids = model.cluster_centers_.argsort()[:, ::-1]
terms = tfidf_vect.get_feature_names()
#print(terms)
#terms"

for i in range(true_k):
    print("----- Cluster: {} -----".format(i))
    for ind in order_centroids[i, :10]:
        print("%s" % terms[ind])


# Find optimal cluster size.

sosd = []
# Run clustering form size 1 to 15 and capture interia.

K = range(1,15)
for k in K:
    km = KMeans(n_clusters = k)
    km = km.fit(X_tfidf)
    sosd.append(km.inertia_)

print("Sum of Square Distance : ", sosd)

# plot
import matplotlib.pyplot as mplib
mplib.plot(K, sosd, 'bx-')
mplib.xlabel('Cluster count')
mplib.ylabel('Sum_of_square_distance')
mplib.title('Elbow method for Optimal cluster size')
mplib.show()


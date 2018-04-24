package alexp.macrobase.outlier.mcod;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Random;

import alexp.macrobase.outlier.mcod.mtree.MTreeClass;
import alexp.macrobase.outlier.mcod.measurements.MesureMemoryThread;
import alexp.macrobase.outlier.mcod.utils.Utils;

public class MicroCluster {
    private double maxDistance = 1; // maxDistance in paper
    private int minNeighborCount = 30; // minNeighborCount in paper
    private int windowSize = 1000; // windowSize in paper
    private int slide = 500;

    private HashMap<Data, ArrayList<MCObject>> micro_clusters = new HashMap<>();

    private HashMap<Data, ArrayList<MCObject>> associate_objects = new HashMap<>();

    private ArrayList<MCObject> PD = new ArrayList<>();

    // store list ob in increasing time arrival order
    private ArrayList<MCObject> dataList = new ArrayList<>();

    private MTreeClass mtree = new MTreeClass();

    private PriorityQueue<MCObject> eventQueue = new PriorityQueue<MCObject>(new MCComparator());

    private ArrayList<MCObject> outlierList = new ArrayList<MCObject>();

    private ArrayList<MCObject> inCluster_objects = new ArrayList<MCObject>();

    private HashSet<Integer> inClusters = new HashSet<>();

    private double numberPointsInClusters = 0;
    private double numberPointsInEventQueue = 0;

    private double avgPointsInRmc = 0;
    private double avgLengthExps = 0;

    public MicroCluster(double maxDistance, int minNeighborCount, int windowSize, int slide) {
        this.maxDistance = maxDistance;
        this.minNeighborCount = minNeighborCount;
        this.windowSize = windowSize;
        this.slide = slide;
    }

    public ArrayList<Data> detectOutlier(ArrayList<Data> data, int currentTime) {
        ArrayList<Data> result = new ArrayList<Data>();
        /*
         * purge expired objects
         */

        long startTime = Utils.getCPUTime();
        ArrayList<MCObject> expiredData = new ArrayList<MCObject>();

        int index = -1;

        for (int i = 0; i < dataList.size(); i++) {

            MCObject d = dataList.get(i);

            if (d.arrivalTime() <= currentTime - maxDistance) {

                index = i;
                expiredData.add(d);

                if (d.isInCluster) {
                    ArrayList<MCObject> inCluster_objects2;
                    if (d.isCenter) {
                        inCluster_objects2 = micro_clusters.get(d);

                    }
                    // update cluster
                    else inCluster_objects2 = micro_clusters.get(d.cluster);
//                   
//                    if (inCluster_objects2 != null) 
//                    {
                    long startTime2 = Utils.getCPUTime();

                    inCluster_objects2.remove(d);


//                        micro_clusters.put(inCluster_objects2.get(0).cluster, inCluster_objects2);
                    MesureMemoryThread.timeForIndexing += Utils.getCPUTime() - startTime2;

                    /*
                     * check if size of cluster shrink below k+1
                     */
                    if (inCluster_objects2.size() < minNeighborCount + 1) {
//                            if(d.isCenter)
//                                micro_clusters.remove(d);
//                            MicroCluster.inCluster_objects.addAll(inCluster_objects2);
                        process_shrink_cluster(inCluster_objects2, currentTime);
//                            MicroCluster.inCluster_objects.clear();
                    }
//                    }

                } else {// d is in PD
                    long startTime2 = Utils.getCPUTime();
                    PD.remove(d);
                    d.Rmc.stream().map((c) -> associate_objects.get(c)).filter((list_associates) -> (list_associates != null)).forEach((list_associates) -> {
                        list_associates.remove(d);
                    });
                    MesureMemoryThread.timeForIndexing += Utils.getCPUTime() - startTime2;
                }

            } else break;
        }
        process_event_queue(expiredData, currentTime);
        for (int i = index; i >= 0; i--) {

            dataList.remove(i);
        }

        MesureMemoryThread.timeForExpireSlide += Utils.getCPUTime() - startTime;

        /*
         * process new incoming data
         */
        // do range query with mtree of cluster centers
        startTime = Utils.getCPUTime();
        data.stream().map((d2) -> new MCObject(d2)).map((d) -> {

            process_data(d, currentTime, false);
            return d;
        }).forEach((d) -> {
            dataList.add(d);
        });


        outlierList.stream().forEach((o) -> {
            result.add(o);
        });
        MesureMemoryThread.timeForNewSlide += Utils.getCPUTime() - startTime;


        if (numberPointsInEventQueue < eventQueue.size())
            numberPointsInEventQueue = eventQueue.size();
        HashSet<Integer> tempTest = new HashSet<>();
        for (Data center : micro_clusters.keySet()) {
            ArrayList<MCObject> l = micro_clusters.get(center);
            for (MCObject o : l) {
                if (o.arrivalTime() >= currentTime - windowSize) {
                    tempTest.add(o.arrivalTime());
                    numberPointsInClusters++;
                }
            }
        }

        dataList.stream().forEach((o) -> {
            avgPointsInRmc += o.Rmc.size();
            avgLengthExps += o.exps.size();
        });
        avgPointsInRmc = avgPointsInRmc / dataList.size();
        avgLengthExps = avgLengthExps / dataList.size();
        System.out.println("#points in clusters: " + numberPointsInClusters);
        return result;

    }

    public void print_cluster() {
        micro_clusters.keySet().stream().map((o) -> {
            System.out.println("Center: " + o.values[0]);
            return o;
        }).map((o) -> {
            System.out.print("Member:");
            return o;
        }).map((o) -> {
            micro_clusters.get(o).stream().forEach((o2) -> {
                System.out.print(o2.values[0] + " ; ");
            });
            return o;
        }).forEach((_item) -> {
            System.out.println();
        });
        System.out.println();
    }

    public void print_outlier() {
        System.out.println("Outliers: ");
        outlierList.stream().forEach((o) -> {
            System.out.print(o.values[0] + " ; ");
        });
        System.out.println();
    }

    public void print_PD() {
        System.out.println();
        System.out.println("PD list: ");
        PD.stream().forEach((o) -> {
            System.out.print(o.values[0] + " ; ");
        });
        System.out.println();
    }

    private void process_shrink_cluster(ArrayList<MCObject> inCluster_objects, int currentTime) {
        long startTime = Utils.getCPUTime();
        mtree.remove(inCluster_objects.get(0).cluster);
        ArrayList<MCObject> list_associates = associate_objects.get(inCluster_objects.get(0).cluster);
        if (list_associates != null) list_associates.stream().forEach((o) -> {
            o.Rmc.remove(inCluster_objects.get(0).cluster);
        });
        associate_objects.remove(inCluster_objects.get(0).cluster);
        micro_clusters.remove(inCluster_objects.get(0).cluster);

        MesureMemoryThread.timeForIndexing += Utils.getCPUTime() - startTime;
        inCluster_objects.stream().map((d) -> {
            d.cluster = null;
            return d;
        }).map((d) -> {
            d.isInCluster = false;
            return d;
        }).map((d) -> {
            d.isCenter = false;
            return d;
        }).map((d) -> {
            d.numberOfSucceeding = 0;
            return d;
        }).map((d) -> {
            d.exps.clear();
            return d;
        }).map((d) -> {
            d.ev = 0;
            return d;
        }).map((d) -> {
            d.Rmc.clear();
            return d;
        }).forEach((d) -> {
            if (d.arrivalTime() > currentTime - windowSize)
                process_data(d, currentTime, true);
        });

    }

    private void addObjectToCluster(MCObject d, MCObject cluster, boolean fromCluster) {


        d.cluster = cluster;
        d.isInCluster = true;

        ArrayList<MCObject> list = micro_clusters.get(cluster);
        if (!list.contains(d))
            list.add(d);
        micro_clusters.put(cluster, list);

        /*
         * evaluate distance between the new object and objects in PD that associate with cluster
         */
        ArrayList<MCObject> objects = associate_objects.get(cluster);

        if (objects != null) objects.stream().forEach((o) -> {
            double distace = mtree.getDistanceFunction().calculate(d, o);
            if (distace <= maxDistance) {
                // increase number if succeeding neighbors
                // o.numberOfSucceeding++;
                if (o.arrivalTime() < d.arrivalTime()) {
                    if (inCluster_objects.contains(o) || !fromCluster) o.numberOfSucceeding++;

                    else {
                        if ((o.arrivalTime() - 1) / slide == (d.arrivalTime() - 1) / slide)
                            d.numberOfSucceeding++;
                        else
                            d.exps.add(o.arrivalTime() + windowSize);
                    }
                } else {
                    if (inCluster_objects.contains(o) || !fromCluster) {
                        if ((o.arrivalTime() - 1) / slide == (d.arrivalTime() - 1) / slide)
                            o.numberOfSucceeding++;
                        else o.exps.add(d.arrivalTime() + windowSize);//?
                    }
                    d.numberOfSucceeding++;
                }
                // check if o is inlier
                if (o.exps.size() + o.numberOfSucceeding >= minNeighborCount && outlierList.contains(o)) {
                    outlierList.remove(o);
                    // add o to event queue
                    if (!o.exps.isEmpty()) {
                        o.ev = min(o.exps);
                        eventQueue.add(o);
                    }
                }
            }
        });

    }

    private void process_data(MCObject d, int currentTime, boolean fromCluster) {

        if (d.arrivalTime() <= currentTime - windowSize) return;
        long startTime = Utils.getCPUTime();
        MTreeClass.Query query = mtree.getNearestByRange(d, maxDistance * 3 / 2);
        MesureMemoryThread.timeForQuerying += Utils.getCPUTime() - startTime;
        // ed

        double min_distance = Double.MAX_VALUE;

        MTreeClass.ResultItem ri = null;

        boolean isFoundCluster = false;

        if (query.iterator().hasNext()) {
            ri = query.iterator().next();

            min_distance = ri.distance;

            if (micro_clusters.get(ri.data) != null && micro_clusters.get(ri.data).size() > 0)
                isFoundCluster = true;
        }

        if (min_distance <= maxDistance / 2 && isFoundCluster && fromCluster == false) {
            // assign to this closet cluster
            MCObject closest_cluster = (MCObject) ri.data;
            long startTime2 = Utils.getCPUTime();
            addObjectToCluster(d, closest_cluster, fromCluster);
            MesureMemoryThread.timeForIndexing += Utils.getCPUTime() - startTime2;
        } else {

            /*
             * do range query in PD and mtree (distance to center <= 3/2R)
             */

            ArrayList<MCObject> neighbor_in_mtree = new ArrayList<>();
            ArrayList<MCObject> neighbor_in_PD = new ArrayList<>();
            ArrayList<MCObject> neighbor_in_3_2Apart_PD = new ArrayList<>();
            ArrayList<MCObject> neighbor = new ArrayList<>();
            ArrayList<MCObject> neighbor_in_R2 = new ArrayList<>();

            for (MTreeClass.ResultItem ri2 : query) {
                if (ri2.distance == 0) d.values[0] += (new Random()).nextDouble() / 1000000;

                /*
                 * scan in cluster to find neighbors
                 */
                d.Rmc.add((MCObject) ri2.data);

                if (associate_objects.get(ri2.data) == null) {
                    ArrayList<MCObject> l = new ArrayList<>();
                    l.add(d);
                    associate_objects.put(ri2.data, l);
                } else {
                    ArrayList<MCObject> l = associate_objects.get(ri2.data);
                    l.add(d);
                    associate_objects.put(ri2.data, l);
                }

                ArrayList<MCObject> object_in_cluster = micro_clusters.get(ri2.data);
                if (object_in_cluster != null)
                    for (MCObject o : object_in_cluster) {

                        if (mtree.getDistanceFunction().calculate(d, o) <= maxDistance) {
                            neighbor_in_mtree.add(o);
                        }
                    }

            }
            PD.stream().forEach((m) -> {
                double distance = mtree.getDistanceFunction().calculate(d, m);

                if (distance <= maxDistance / 2) neighbor_in_R2.add(m);
                if (distance <= maxDistance) {
                    neighbor_in_PD.add(m);
                    neighbor_in_3_2Apart_PD.add(m);
                } else if (distance <= maxDistance * 3 / 2) neighbor_in_3_2Apart_PD.add(m);
            });
            neighbor.addAll(neighbor_in_PD);
            neighbor.addAll(neighbor_in_mtree);

            neighbor_in_PD.stream().map((o) -> {
                if (o.arrivalTime() < d.arrivalTime()) {
                    if (inCluster_objects.contains(o) || !fromCluster) o.numberOfSucceeding++;

                    else {
                        if ((o.arrivalTime() - 1) / slide == (d.arrivalTime() - 1) / slide)
                            d.numberOfSucceeding++;
                        else
                            d.exps.add(o.arrivalTime() + windowSize);
                    }
                } else {
                    if (inCluster_objects.contains(o) || !fromCluster) {
                        if ((o.arrivalTime() - 1) / slide == (d.arrivalTime() - 1) / slide)
                            o.numberOfSucceeding++;
                        else o.exps.add(d.arrivalTime() + windowSize);
                    }
                    d.numberOfSucceeding++;
                }
                /*
                 * check for o becomes inlier
                 */
                return o;
            }).filter((o) -> (o.numberOfSucceeding + o.exps.size() >= minNeighborCount && outlierList.contains(o))).map((o) -> {
                outlierList.remove(o);
                return o;
            }).map((o) -> {
                if (o.exps.size() > 0) o.ev = min(o.exps);
                return o;
            }).forEach((o) -> {
                eventQueue.add(o);
            });
            neighbor_in_mtree.stream().forEach((o) -> {
                if (o.arrivalTime() < d.arrivalTime()) {
                    if ((o.arrivalTime() - 1) / slide == (d.arrivalTime() - 1) / slide)
                        d.numberOfSucceeding++;
                    else
                        d.exps.add(o.arrivalTime() + windowSize);
                } else {
                    d.numberOfSucceeding++;
                }
            });


            if (neighbor_in_R2.size() > minNeighborCount * 1.1 && !fromCluster) {
                long startTime2 = Utils.getCPUTime();

                // form cluster
                d.isCenter = true;
                d.isInCluster = true;
                neighbor_in_R2.add(d);
                for (MCObject o : neighbor_in_R2) {
                    if (o.isInCluster && o.arrivalTime() != d.arrivalTime())
                        System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
                    o.isCenter = false;
                    o.cluster = d;
                    o.isInCluster = true;
                    o.numberOfSucceeding = 0;
                    o.exps.clear();
                    if (inClusters.contains(o.arrivalTime())) System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAA");
                    inClusters.add(o.arrivalTime());
                    PD.remove(o);
                    eventQueue.remove(o);
                    outlierList.remove(o);
                }

                if (!micro_clusters.keySet().contains(d)) {
                    micro_clusters.put(d, neighbor_in_R2);
                    mtree.add(d);
                }
//                else micro_clusters.get(d).addAll(neighbor_in_R2);

                // update Rmc for points in PD
                neighbor_in_3_2Apart_PD.stream().forEach((o) -> {
                    o.Rmc.add(d);
                });
                associate_objects.put(d, neighbor_in_3_2Apart_PD);
                MesureMemoryThread.timeForIndexing += Utils.getCPUTime() - startTime2;
            } else // add to event queue and pd
            {

                PD.add(d);

                Collections.sort(d.exps, Collections.reverseOrder());
                for (int i = d.exps.size() - 1; i >= minNeighborCount - d.numberOfSucceeding && i >= 0; i--)

                    d.exps.remove(i);

                if (d.numberOfSucceeding + d.exps.size() < minNeighborCount) {

                    outlierList.add(d);

                } else if (d.numberOfSucceeding + d.exps.size() >= minNeighborCount && d.exps.size() > 0) {

                    /**
                     * keep k most recent preceeding neighbors
                     */

                    d.ev = min(d.exps);
                    long startTime2 = Utils.getCPUTime();
                    eventQueue.add(d);
                    MesureMemoryThread.timeForIndexing += Utils.getCPUTime() - startTime2;
                }
            }

        }

        Utils.computeUsedMemory();
    }

    private void process_event_queue(ArrayList<MCObject> expireData, int currentTime) {
        MCObject x = eventQueue.peek();

        while (x != null && x.ev <= currentTime) {

            x = eventQueue.poll();

            for (int i = x.exps.size() - 1; i >= 0; i--) {
                if (x.exps.get(i) <= currentTime) x.exps.remove(i);
            }

            if (x.exps.size() + x.numberOfSucceeding < minNeighborCount) {

                outlierList.add(x);

            } else if (x.exps.size() > 0) {
                x.ev = min(x.exps);
//                long startTime3 = Utils.getCPUTime();
                eventQueue.add(x);
//                MesureMemoryThread.timeForIndexing += Utils.getCPUTime() - startTime3;
            }

            x = eventQueue.peek();
        }

        expireData.stream().forEach((p) -> {
            outlierList.remove(p);
        });

        outlierList.stream().map((outlierList1) -> (MCObject) outlierList1).forEach((d) -> {
            for (int k = d.exps.size() - 1; k >= 0; k--) {
                if (d.exps.get(k) <= currentTime) d.exps.remove(k);
            }
        });

    }

    private int min(ArrayList<Integer> exps) {
        int min = exps.get(0);
        for (Integer i : exps)
            if (i < min) min = i;
        return min;
    }

}

class MCComparator implements Comparator<MCObject> {

    @Override
    public int compare(MCObject o1, MCObject o2) {
        if (o1.ev < o2.ev) return -1;
        else if (o1.ev == o2.ev) return 0;
        else return 1;

    }

}

class MCObject extends Data {

    MCObject cluster;
    ArrayList<Integer> exps;
    ArrayList<MCObject> Rmc;

    int ev;
    boolean isInCluster;
    boolean isCenter;

    int numberOfSucceeding;

    MCObject(Data d) {
        super(d.arrivalTime(), d.values);

        exps = new ArrayList<>();
        Rmc = new ArrayList<>();
        isCenter = false;
        isInCluster = false;
    }

}
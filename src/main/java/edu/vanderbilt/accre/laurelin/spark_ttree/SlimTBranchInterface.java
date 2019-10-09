package edu.vanderbilt.accre.laurelin.spark_ttree;

import edu.vanderbilt.accre.laurelin.Cache;
import edu.vanderbilt.accre.laurelin.array.ArrayBuilder;
import edu.vanderbilt.accre.laurelin.root_proxy.ROOTFileCache;
import edu.vanderbilt.accre.laurelin.root_proxy.TBranch;
import edu.vanderbilt.accre.laurelin.spark_ttree.SlimTBranch.SlimTBasket;

public interface SlimTBranchInterface {

    long[] getBasketEntryOffsets();

    SlimTBasket getBasket(int basketid);

    void addBasket(SlimTBasket basket);

    String getPath();

    TBranch.ArrayDescriptor getArrayDesc();

    /**
     * Glue callback to integrate with edu.vanderbilt.accre.laurelin.array
     * @param basketCache the cache we should be using
     * @param fileCache
     * @return GetBasket object used by array
     */
    ArrayBuilder.GetBasket getArrayBranchCallback(Cache basketCache, ROOTFileCache fileCache);

}
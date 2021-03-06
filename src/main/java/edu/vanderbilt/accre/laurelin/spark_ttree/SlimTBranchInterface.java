package edu.vanderbilt.accre.laurelin.spark_ttree;

import com.google.common.collect.ImmutableRangeMap;

import edu.vanderbilt.accre.laurelin.array.ArrayBuilder;
import edu.vanderbilt.accre.laurelin.cache.BasketCache;
import edu.vanderbilt.accre.laurelin.root_proxy.TBranch;
import edu.vanderbilt.accre.laurelin.root_proxy.io.ROOTFileCache;

public interface SlimTBranchInterface {

    long[] getBasketEntryOffsets();

    SlimTBasket getBasket(int basketid);

    TBranch.ArrayDescriptor getArrayDesc();

    /**
     * Glue callback to integrate with edu.vanderbilt.accre.laurelin.array
     * @param basketCache the cache we should be using
     * @param fileCache what file handle cache we should be using
     * @return GetBasket object used by array
     */
    ArrayBuilder.GetBasket getArrayBranchCallback(BasketCache basketCache, ROOTFileCache fileCache);

    ImmutableRangeMap<Long, Integer> getRangeToBasketIDMap();

}
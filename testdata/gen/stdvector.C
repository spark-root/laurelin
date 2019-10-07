// from https://root.cern.ch/doc/master/hvector_8C_source.html
/// \file
/// \ingroup tutorial_tree
/// \notebook
/// Write and read STL vectors in a tree.
///
/// \macro_image
/// \macro_code
///
/// \author The ROOT Team

#include <vector>

#include "TFile.h"
#include "TTree.h"
#include "TCanvas.h"
#include "TFrame.h"
#include "TH1F.h"
#include "TBenchmark.h"
#include "TRandom.h"
#include "TSystem.h"

void write()
{

    TFile *f = TFile::Open("stdvector.root","RECREATE");

    if (!f) { return; }

    std::vector<float> vpx;
    std::vector<float> vpy;
    std::vector<float> vpz;
    std::vector<float> vrand;
    std::vector<char> charv;
    std::vector<short> shortv;
    std::vector<int> intv;
    std::vector<long> longv;
    std::vector<double> doublev;


    // Create a TTree
    TTree *t = new TTree("tvec","Tree with vectors");
    t->Branch("vpx",&vpx);
    t->Branch("vpy",&vpy);
    t->Branch("vpz",&vpz);
    t->Branch("vrand",&vrand);
    t->Branch("char",&charv);
    t->Branch("short",&shortv);
    t->Branch("int",&intv);
    t->Branch("long",&longv);
    t->Branch("double",&doublev);

    gRandom->SetSeed(2072019);
    for (Int_t i = 0; i < 10; i++) {
        Int_t npx = (Int_t)(gRandom->Rndm(1)*15);

        vpx.clear();
        vpy.clear();
        vpz.clear();
        vrand.clear();
        charv.clear();
        shortv.clear();
        intv.clear();
        longv.clear();
        doublev.clear();

        for (Int_t j = 0; j <= i; ++j) {
            int val = 10 * i + j;
            Float_t px,py,pz;
            gRandom->Rannor(px,py);
            pz = px*px + py*py;
            Float_t random = gRandom->Rndm(1);

            vpx.emplace_back(px);
            vpy.emplace_back(py);
            vpz.emplace_back(pz);
            vrand.emplace_back(random);
            charv.emplace_back(val);
            shortv.emplace_back(val);
            intv.emplace_back(val);
            longv.emplace_back(val);
            doublev.emplace_back(val);
        }

        t->Fill();
        if (i % 3 == 1) {
            t->FlushBaskets();
        }
    }
    f->Write();

    delete f;
}

void stdvector()
{
    write();
}

/* ROOT plot maker for smartsubmit test data
Bobak Hashemi -- March 08 2016
*/

#include "TChain.h"
#include "TCanvas.h"
#include "TString.h"
#include "TTree.h"

void rootplotmaker(TString fname, TString run_num="1")
{
	TTree *t = new TTree();
	t->ReadFile(fname);

	t->Draw("REALTIME>>runHist");
	TH1F *runHist = new TH1F("runHist");
	runHist->SetTitle("Root Script Runtime for Smartsubmit Run "+run_num+", 4 .root files per job");
	runHist->SetYTitle("Number of Jobs");
	runHist->SetXTitle("Real Time (seconds)");

	runHist->Draw();
	gPad->SaveAs("runHist.png");
}
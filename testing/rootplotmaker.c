/* ROOT plot maker for smartsubmit test data
Bobak Hashemi -- March 08 2016
*/

#include "TChain.h"
#include "TCanvas.h"
#include "TString.h"
#include "TTree.h"

void rootplotmaker(TString fname, TString run_num="1")
{
	TCanvas *BG = new TCanvas("c1", "default", 1920, 1080);
	TTree *t = new TTree();
	t->ReadFile(fname);

	TH1F *runHist = new TH1F("runHist", "Root Script Runtime for Smartsubmit Run "+run_num+", 4 .root files per job", 500,500,2500);
	runHist->SetYTitle("Number of Jobs");
	runHist->SetXTitle("Real Time (seconds)");

	t->Draw("REALTIME>>runHist");
	runHist->Draw();
	gPad->SaveAs("runHist.png");
}
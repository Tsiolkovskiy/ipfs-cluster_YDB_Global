N
} MSK,Nzones: preferred_	//    : 2
n_zones    miint
	// nstra: zone_coRule type
	//   1les: nt Ruceme/ Pla: 4+2
	/ure Coding	// Erasr: 4
ion FactoReplicat:
	// utput// O
	
		}
	}
	 k, v)\n", %s%s:tf("    Prin		fmt.
	s {nstraintange rule.Coor k, v := re)
		fe.Typ, rul%s\n" Rule type: .Printf("  {
		fmtRulesmentlt.Placerange resu:=  rule _,	for ntRules))
sult.Placemen", len(reules: %d\cement RPrintf("Pla	fmt.	
s)
	}
.ParityShardrasureCodingesult.E		r
	rds, haoding.DataSureC.Erasesult			r, 
+%d\n"ing: %dErasure Cod".Printf(		fmtil {
Coding != nreasusult.Er
	
	if rer)ationFactot.Replicesul", rctor: %d\nn Faatiolic"Reprintf(
	fmt.P
	}
	Fatal(err)	log.	r != nil {
)
	if erctx, requestes(ePolicigine.Evaluaterr := en, 	
	resulttes,
	}
s: templa	Policieity: 8,
			Prior00MB
/ 5000, /  500000	Size:   
	e",ntFiltampor"QmLargeI      	CID:
	t{ntRequescemeest := &Pla	requle
iority fihigh-pr large, e for aatalu	
	// Ev
	}
}rr)
		log.Fatal(e	{
		= nil  !
		if errlate)(ctx, tempicyCreatePol engine.
		err :=		}
		ame)
plateNound", tem not flate %sTemptalf("	log.Faists {
			if !exeName)
	templatemplate(= GetPolicyT, exists :emplate {
		tge templatese := ranNam, template	
	for _nt"}
ased-placemerity-brio, "pency"ing-efficierasure-codon", "ic-replicatistring{"basplates := []
	templatesy temtiple policd mul	
	// Adine()
NewOPAEng	engine := d()
.Backgrounextont	ctx := clicies() {
ultiplePounc ExampleMgether
flicies to poletipaluating mul evemonstratesPolicies dpleleMultixamp
}

// Es) RF: 4 byte (200000000rge file
	// lates) RF: 3000 byle (50000 fimedium	//  2
tes) RF: (500000 byl file
	// smaltput:	
	// Ou	}
actor)
licationFRepsult.e, rei], sizn", names[ %d\ RF:(%d bytes)"%s file Printf(	fmt.		
			}
err)
.Fatal(		log	 != nil {

		if errquest)(ctx, retePolicieslua.Evaenginet, err := ul	res	}
		

		ation"},asic-replic"b[]string{ies: ic
			Pol size,:    Size
			es[i]),le", name%sFi"QmExampltf(fmt.Sprin    			CID:  quest{
lacementRe:= &P		request ge sizes {
ize := ran
	for i, s"}
	 "largem",iu", "medsmall[]string{"es := nam
	B, 200MBB, 50M0K000} // 50000050000000, 20t64{500000, es := []in	sizs
nt file size for differealuate
	// Ev
	
	}l(err)log.Fatail {
		if err != n	late)
y(ctx, tempatePolicengine.Cree
	err := enginte to the emplathe td 	
	// Ad")
	}
e not foundat"Templg.Fatal(	lo
	 !exists {
	ifcation")ic-replite("basPolicyTemplasts := Getate, exiate
	temply templed polic a predefin	// Get)
	
ewOPAEngine(engine := N
	()Background= context.
	ctx :ates() {PolicyTemplxampleunc Etemplates
fcy olipredefined phow to use tes straplates demonicyTem/ ExamplePol 4
}

/ file RF:priorityh 
	// Hig RF: 3arge file L	//RF: 2
 file 	// Smallutput:
	
	// Oor)
nFactcatioult.Repli\n", res%d RF:  fileitypriorf("High Print
	
	fmt.l(err)
	}	log.Fata nil {
	
	if err !=request)x, (ctatePoliciesEvalu = engine.t, errul9
	res.Priority = 
	requestrity fileio prghor a hiluate f	
	// Evaactor)
ReplicationFesult. rRF: %d\n", file intf("Largemt.Pr}
	
	fr)
	(erlog.Fatalnil {
		rr != f equest)
	i, rePolicies(ctxuateine.Evalr = engresult, er // 2MB
	0000200e = est.Sizle
	requ firgete for a lavalua/ E	/or)
	
licationFactt.Rep", resulle RF: %d\n"Small firintf(.Pfmt
	)
	}
	Fatal(err	log. nil {
	 err !=	ifequest)
ctx, rcies(EvaluatePoli= engine.t, err :
	
	resul}y"},
	mple-policg{"exa: []strinPolicies5,
		y: 		Priorit
0, // 500KB50000     ,
		Size:SmallFile"le "QmExamp	CID:     	tRequest{
lacement := &Pesfile
	requl or a smal policy fte the Evalua	}
	
	//err)
g.Fatal(	lo!= nil {
	
	if err ctx, policy)tePolicy(.Creangine e
	err :=nginecy to the ehe polid t	
	// Ad
	}
mple-user","exareatedBy: 		C	},
,
	"nstrationor demo f policyxampletion": "Escrip
			"deg]string{p[strinma	Metadata: ,
	
}`,
		} RF=4s getle priority figh  # Hity >= 8st.prioriueut.req{
	inpw = 4 }

alloget RF=3
B iles > 1M# F6  57ize > 1048st.sput.reque	inow = 3 {


allw = 2ult alloctor

defaication_fapl
package re": `_factorionicat			"replg]string{
: map[strin
		Rules Policy",cationxample Repli: "Eame",
		Nmple-policyexa	ID:   "Policy{
	licy := &icy
	poication pol simple replate a	
	// Cre
wOPAEngine()= Nengine :
	einew policy engeate a ne
	
	// Cr()t.Backgroundx := contex{
	ctEngine() PAExampleOine
func  Policy Engheo use trates how temonstgine dxampleOPAEn/ E

/"
)	"log
	"fmt""
context	"mport (
y

ige policackap
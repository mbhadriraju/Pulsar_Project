#!/bin/bash
#
# Script to import upds, derive parameters and convert PSRCAT v2 to v1
#

VER=2.6.1
UPDS_COMPLETED=./upds_completed

# NOTES:
# Input cab_fixw.txt first to add widths
# 

UPDS="aaa+05d.upd,aaa+09c.upd,aaa+09d.upd,aaa+09e.upd,aaa+09f.upd,aaa+09i.upd,aaa+09.upd,aaa+10b.upd,aaa+10d.upd,aaa+10g.upd,aaa+10m.upd,aaa+13.upd,aab+06b.upd,aab+06c.upd,aab+06d.upd,aab+06.upd,aab+07.upd,aab+14.upd,aab+21a.upd,aab+21b.upd,aab+22.upd,aac+18.upd,abb+15.upd,abb+18.upd,abs86.upd,abw+11.upd,acd+19.upd,acj+96.upd,afm+22.upd,aft94.upd,afw+13.upd,agk+90.upd,agr+11.upd,ags+12.upd,akb+10.upd,akc+13.upd,akh+13.upd,akn+13.upd,akts16.upd,al81.upd,amm+21.upd,and92.upd,and93.upd,antt94.upd,apk14.upd,apr+23.upd,aps+17.upd,ar18.upd,ar_drift21.upd,asr+09.upd,asr+15.upd,avk+12.upd,awb+21.upd,awd+12.upd,awkp97.upd,bac+16.upd,bar+23.upd,bb10.upd,bbb+11a.upd,bbb+11.upd,bbb+12.upd,bbb+13.upd,bbc+22.upd,bbf83.upd,bbf84.upd,bbgt02.upd,bbj+11.upd,bbl+94.upd,bbm+95.upd,bbn+11.upd,bbs+95.upd,bbv08.upd,bcc+04.upd,bcf+17.upd,bck+13.upd,bckf06.upd,bcm+16.upd,bdd+15.upd,bdf+17.upd,bdp+03.upd,bfb+16.upd,bfg+03.upd,bfk+17.upd,bfrs18.upd,bgc+13.upd,bgh+06.upd,bgt+20.upd,bgt+21.upd,bhc+17.upd,bhe+19.upd,bhl+94.upd,bjb+97.upd,bjd+06.upd,bjd+11.upd,bjk+05.upd,bjk+20.upd,bjl+11.upd,bjs+16.upd,bk11.upd,bkb+16.upd,bkb+95.upd,bkh+82.upd,bkk+16.upd,bkk+19.upd,bkl+13.upd,bkr+13.upd,blr+13.upd,bls+18.upd,bmj+16.upd,bmk+90a.upd,bmk+90b.upd,bph+17a.upd,bph+17.upd,br14.upd,brf+22.upd,brg99.upd,bri+06.upd,brj+21.upd,brr+07.upd,brr+13.upd,brrk02.upd,brs+19.upd,bs20.upd,bsa+22.upd,bsb+19.upd,bsp+03.upd,bt93.upd,bt99.upd,btb+15.upd,btd82.upd,btgg03.upd,btlb00.upd,bv21.upd,bvr+13.upd,bvs+11.upd,bvvh03.upd,bwt+04.upd,cam95a.upd,cb04.upd,cbb+23.upd,cbcp14.upd,cbm+01.upd,cbmp17.upd,cbmt96.upd,cbp+12.upd,cbp+23.upd,cbv+09.upd,ccb+20.upd,cck+16.upd,cck+18.upd,ccl+01.upd,ccl+21.upd,ccl+69.upd,ccv+04.upd,cdj+20.upd,cdk88.upd,cdm+10.upd,cdt69.upd,cfi+19.upd,cfl+06.upd,cfpd06.upd,cfr+20.upd,cgb05.upd,cgcf00.upd,cgj+11.upd,cgk+01.upd,cgk98.upd,cgm+07.upd,cha03.upd,chmm89.upd,cjlm87.upd,ckl+00.upd,ckm+01.upd,ckr+12.upd,ckr+15.upd,cks+07.upd,cks+92.upd,cl02.upd,cl86.upd,cla17.upd,clb+02.upd,clf+00.upd,clge02.upd,clh+20.upd,clj+92.upd,clm+01.upd,clm+04.upd,clm+05.upd,clm+98.upd,cls+13.upd,cls68.upd,cmc04.upd,cmg+02.upd,cmgl02.upd,cmh91.upd,cmj+05.upd,cmk01.upd,cml+04.upd,cml05.upd,cmn+00.upd,cmr+75.upd,cms+21.upd,cn95.upd,cng+09.upd,cnkc21.upd,cnst96.upd,cnt93.upd,cnt96.upd,cnv+21.upd,cor86.upd,cp68.upd,cpb+23.upd,cpf+15.upd,cpkm02.upd,cpl+06.upd,cpw+15.upd,cpw+16.upd,cpw+18.upd,crc+12.upd,crf+18.upd,crg+06.upd,crgl09.upd,crh+06a.upd,crh+06.upd,crh+16.upd,crhr07.upd,crhr21.upd,crj+08.upd,crl+08.upd,crl93.upd,cro20.upd,crr+09.upd,crr+16.upd,csl+02.upd,csl+12.upd,csl68.upd,csm+20.upd,ct07.upd,cuf65.upd,cvf+23.upd,cvh+14.upd,cwb85.upd,cwp+17.upd,dab+12.upd,dbkl15.upd,dbl+93.upd,dbr+04.upd,dbt09.upd,dbtb82.upd,dcl09.upd,dcl+16.upd,dcm+09.upd,dcm+14.upd,dcm+23.upd,dcrh12.upd,dcs+22.upd,dcww88.upd,ddb+17.upd,ddf+20.upd,ddls20.upd,ddm97.upd,dds+23.upd,dfc+12.upd,dfl+08.upd,dgb+19.upd,dhm+15.upd,djk+20.upd,dk06.upd,dk14.upd,dkg08.upd,dkg09.upd,dkj+14.upd,dkm+01.upd,dl70.upd,dlm+01.upd,dlp70.upd,dlrm03.upd,dls72.upd,dls73.upd,dma+93.upd,dmd+88.upd,dmk+93.upd,dml02.upd,dml+11.upd,dpf+02.upd,dpm+01a.upd,dpm+01.upd,dpr+10.upd,dpr+22.upd,dr21.upd,drb+04.upd,drc+16.upd,drc+21.upd,drk+21.upd,drr11.upd,dsb+98.upd,dsm+13.upd,dsm+16.upd,dsw+17.upd,dtbr09.upd,dth78.upd,dtms88.upd,dtws85.upd,dvk+16.upd,dwnc18.upd,dyc+14.upd,dym+20.upd,eb01b.upd,eb01.upd,ebvb01.upd,efk+13.upd,egc+02.upd,egc+13.upd,egh+01.upd,eit+11.upd,ejb+11.upd,ekl09.upd,ekl+80.upd,eklk13.upd,elk+11.upd,elsk11.upd,emk+10.upd,erb+20.upd,esy+20.upd,etm+09.upd,evh+22.upd,ezy+08.upd,faa+11.upd,fak15.upd,fau04.upd,fb06.upd,fbb+90.upd,fbw+11.upd,fc89.upd,fck+03.upd,fcp+21.upd,fcwa95.upd,fdr15.upd,ffb91.upd,fg81.upd,fgbc99.upd,fgl+92.upd,fgml97.upd,fgri04.upd,fgw94.upd,fhc+14.upd,fhm+69.upd,fhn+05.upd,fjmi16.upd,fk91.upd,fkb99.upd,fkv93.upd,fla91.upd,fla93.upd,fla94a.upd,fla94b.upd,fos90.upd,fpds01.upd,frb+08.upd,fre08.upd,frg07.upd,frk+17.upd,frm+18.upd,frs93.upd,fsk+04.upd,fsk+10.upd,fsk+14.upd,fss73.upd,fst14.upd,fst88.upd,ft14.upd,ftb+88.upd,fwc93.upd,fwe+12.upd,gac+21.upd,gae14.upd,gaia18.upd,gbv+21.upd,gcl+10.upd,gdg+00.upd,gdk11.upd,geg10.upd,gfb+22.upd,gfc+12.upd,gfg+21.upd,gg07.upd,gg74.upd,ggts88.upd,gh07.upd,gh08.upd,gh09b.upd,gh09.upd,gha13.upd,ghd02.upd,ghe+01.upd,ghs05.upd,ghtm11.upd,gk98.upd,gkc+07.upd,gkj+13.upd,gl98.upd,gmga05.upd,goc+19.upd,gpg01.upd,gr78.upd,grf+22.upd,gsa+21.upd,gsf+11.upd,gsfj98.upd,gsk+03.upd,gsl+16.upd,gth+14.upd,gvb+23.upd,gvbt00.upd,gvc+04.upd,gvo+17.upd,gwk08.upd,gwk+10.upd,hab07.upd,han87.upd,har10a.upd,har96.upd,hb07a.upd,hb07b.upd,hb07c.upd,hbp+68.upd,hcb+07.upd,hcb+22.upd,hcg+01.upd,hcg03.upd,hcg07.upd,hfs+04.upd,hg10a.upd,hg10b.upd,hg10.upd,hg15.upd,hg73.upd,hgc+04.upd,hgc+09.upd,hglh01.upd,hgp+20.upd,hh92.upd,hhss02.upd,hjm+70.upd,hl87.upd,hla93.upd,hlk+04.upd,hllk05.upd,hly+20.upd,hmb+97.upd,hml+06.upd,hmp98.upd,hmq99.upd,hmvd18.upd,hmz+04.upd,hng+08.upd,hnl+17.upd,hns10.upd,hok+09.upd,hra+12.upd,hrk+08.upd,hrm+11.upd,hrr+04.upd,hrs+06.upd,hrs+07.upd,hsh+12.upd,hsp+03.upd,ht69.upd,ht74.upd,ht75a.upd,ht75b.upd,htg+14.upd,htg+68.upd,htv+01.upd,hww+21.upd,hwxh16.upd,hys+21.upd,icm+05.upd,icp+03.upd,ics+02.upd,ims+04.upd,ims94.upd,irz+14.upd,jac05.upd,jah+16.upd,jbb+15.upd,jbb+16.upd,jbo+07.upd,jbo+09.upd,jbv+03.upd,jbv+19.upd,jcj+06.upd,jcs+17.upd,jcv+06.upd,jh05.upd,jhb+05.upd,jhgm02.upd,jhv+05.upd,jk18.upd,jkc+18.upd,jkk+07.upd,jkk+20.upd,jkl+06.upd,jks21.upd,jl21.upd,jl88.upd,jlh+93.upd,jlm+92.upd,jml+09.upd,jml+92.upd,jml+94.upd,jml+95.upd,jmr+21.upd,joh90.upd,jpm+22.upd,jrr+15.upd,js06.upd,jsb+09.upd,jsb+10.upd,jsd+21.upd,jsk+08.upd,jvk+18.upd,jw06.upd,kac+10.upd,kac+73.upd,kapw91.upd,kas12.upd,kbc+22.upd,kbd+14.upd,kbj+18.upd,kbk+13.upd,kbkr23.upd,kblr21.upd,kbm+03.upd,kbm+96.upd,kbm+97.upd,kbt+19.upd,kbv+13.upd,kcb+88.upd,kcj+12.upd,kcm+98.upd,kd16.upd,kdk+20.upd,kdl+19.upd,kds+15.upd,kds+98.upd,kek+13.upd,kel+09.upd,ker19.upd,ker96.upd,kg03.upd,kgc+01.upd,kggl01.upd,kh15.upd,khs+14.upd,khvb98.upd,kjb+12.upd,kjk+08.upd,kjr+11.upd,kjv+10.upd,kk09b.upd,kk09.upd,kkk03.upd,kkl+09.upd,kkl+11.upd,kkl+15.upd,kkm+03.upd,kkn+16.upd,kkp+12.upd,kkv02.upd,kkv+15.upd,kkvh11.upd,kl01.upd,kl07.upd,kla+11.upd,kle+10.upd,klgj03.upd,klh+03.upd,kll+99.upd,klm+00a.upd,klm+00.upd,kls+15.upd,kmj+96.upd,kmk+18.upd,kmn+15.upd,kom74.upd,kp07.upd,kpg06a.upd,kpg06b.upd,kpg07.upd,kr00.upd,ksh+99.upd,ksm+06.upd,ksr+12.upd,ktr94.upd,kul86.upd,kv05a.upd,kv05.upd,kv11.upd,kva07.upd,kvf+14.upd,kvh+15.upd,kvk+14.upd,kw03.upd,kw90.upd,kws03.upd,kxl+98.upd,kzz+18.upd,laks15.upd,lan69.upd,las82.upd,lbb+10.upd,lbb+88.upd,lbh+15.upd,lbhb93.upd,lbk+04.upd,lbm+87.upd,lbr+13.upd,lbs+20.upd,lcf+96.upd,lcm+00.upd,lcm13.upd,lcx02.upd,ldk+13.upd,lem+15.upd,lfa+16.upd,lfl+06.upd,lfmp02.upd,lfrj12.upd,lgr13.upd,lh81.upd,lhh+13.upd,lhl+14.upd,lht14.upd,lin18.upd,ljd+21.upd,ljg+15.upd,lk11.upd,lkb+11.upd,lkf+21.upd,lkg05.upd,lkg10.upd,lkgk06.upd,lkk15.upd,lkm+00.upd,lkm+01.upd,ll76.upd,llb+96.upd,llc98.upd,lma+04.upd,lmbm00.upd,lmcs07.upd,lmd+90.upd,lmd96.upd,lmj+16.upd,lmk+09.upd,lml+98.upd,lmm+13.upd,lnk+11.upd,lnl+95.upd,lor94.upd,lot+23.upd,lppg22.upd,lps93.upd,lr11.upd,lr77.upd,lrc+09.upd,lrfs11.upd,ls69.upd,lsb+17.upd,lsf+06.upd,lsf+17.upd,lsg+15.upd,lsgm09.upd,lsjb20.upd,lsk+15.upd,lsk+18.upd,ltk+14.upd,lvm68.upd,lvw68.upd,lvw69a.upd,lvw69b.upd,lwa+02.upd,lwf+04.upd,lws+22.upd,lwy+16.upd,lwy+21.upd,lx00.upd,lxf+05.upd,lylg95.upd,lyn87.upd,lys+20.upd,lys+23.upd,lyy+19.upd,lzb+00.upd,lzc95.upd,lzg+11.upd,lzz+21.upd,maa15.upd,mac+02.upd,mad12.upd,mam+06.upd,man74.upd,mav17.upd,mb75.upd,mbc+02.upd,mbc+19.upd,mbc+20.upd,mbp+14.upd,mbs+22.upd,mca00.upd,mcc+06.upd,mcn71.upd,mds+18.upd,mdt85.upd,met+06.upd,met+21.upd,mfb+20.upd,mfl+06.upd,mgf+19.upd,mgi+79.upd,mgm+04.upd,mgr+05.upd,mgz+13.upd,mgz+98.upd,mh04.upd,mh11.upd,mhah83.upd,mhb+13.upd,mhl+02.upd,mhl+18.upd,mhmk90.upd,mhp+14.upd,mic13.upd,mig00.upd,miv+20.upd,mj95.upd,mjs+16.upd,mka+73.upd,mke+20.upd,mkhr87.upd,mkn+17.upd,mks05.upd,mks+10.upd,mkz+03.upd,ml90.upd,mlb+12.upd,mlc+01.upd,mld+90.upd,mld+96.upd,mlj+89b.upd,mlk+09.upd,mll+06.upd,mlr+91.upd,mlt+78.upd,mmh+91.upd,mml+93.upd,mmtl15.upd,mmw+06.upd,mnd85.upd,mnf+16.upd,mngh78.upd,moh83.upd,mop93.upd,mp85.upd,mpk10.upd,mpr+17.upd,mr01a.upd,mrd+22.upd,mrg+07.upd,msf+15.upd,msf+17.upd,msh+05.upd,msk+03.upd,msk+12.upd,msl+10.upd,mss+02.upd,mss+20.upd,mt74.upd,mtb+18.upd,mte+05.upd,mth72.upd,mvk+06.upd,mwkj03.upd,mzl+23.upd,naa+11.upd,nab+13.upd,naf03.upd,nag+22.upd,nbb+14.upd,nbf+95.upd,nbg+12.upd,ncb+15.upd,ncb+19.upd,nck+20.upd,ndk+20.upd,nft95.upd,ng06.upd,ngf+20.upd,nic99.upd,nie+20.upd,nj98.upd,njkk08.upd,nkt+18.upd,nll+95.upd,nmc81.upd,npn+20.upd,nr06.upd,nrr05.upd,nsk08.upd,nsk+15.upd,nss01.upd,nst96.upd,nt92.upd,ntf93.upd,ocg+18.upd,of93.upd,ofb+11.upd,ojk+08.upd,oklk10.upd,okp+21.upd,osh+23.upd,ovm+20.upd,pab+18.upd,pakw91.upd,palfa21.upd,pb15.upd,pbh+17.upd,pbh+21.upd,pbk11.upd,pbm+19.upd,pc15.upd,pdb+15.upd,pdc+05.upd,pdf+10.upd,pdm+03.upd,pfb+13.upd,pga+12a.upd,pga+12.upd,pga+13.upd,pgf+12.upd,phbc68.upd,phl+16.upd,pkb08.upd,pkgw07.upd,pkj+13.upd,pkj+21.upd,pkr+19.upd,pks02.upd,pkw+03.upd,plca18.upd,pld+13.upd,plmg93.upd,pmd+12.upd,pmq+21.upd,ppp+09.upd,pqm+21.upd,prf+14.upd,prl+20.upd,psf+22.upd,psj+19.upd,pv91.upd,pvc19.upd,qmlg95.upd,qpl+19.upd,qyb+15.upd,rap+12.upd,rb08.upd,rb13.upd,rbkl21.upd,rbr14.upd,rcf+06.upd,rch+19.upd,rcl+13.upd,rdga95.upd,rei+11.upd,rfg+22.upd,rfgr19.upd,rfk+96.upd,rfs+12.upd,rft+16.upd,rgf+21.upd,rgfz16.upd,rgh+01.upd,rgk+93.upd,rgl12.upd,rhc+16.upd,rhr+02.upd,rhs+05.upd,rip+13.upd,rjgk06.upd,rkf+21.upd,rkf+22.upd,rkp+11.upd,rl94.upd,rlm+95.upd,rm05.upd,rmd+97.upd,rmg+09.upd,rmg+10.upd,rnc+22.upd,rom12.upd,rpp+20.upd,rrb+15.upd,rrc+11.upd,rrc+13.upd,rrj01.upd,rrk01.upd,rrl+18.upd,rs11.upd,rsa+14.upd,rsb+04.upd,rsc+10.upd,rsc+21.upd,rsm+13.upd,rtj+07.upd,rtj+96.upd,rtk+03.upd,rvh+15.upd,rvi+14.upd,saa+23.upd,sab+16.upd,san16.upd,sbb+18.upd,sbb96.upd,sbc+19.upd,sbc+21.upd,sbd+11.upd,sbg+19.upd,sbl+96.upd,sbm+22.upd,sbo+22.upd,sbs+21.upd,sbs+22.upd,scb+19.upd,scc+15.upd,scg+04.upd,scs+17.upd,scs86.upd,sdz+10.upd,sef+16.upd,sfa+19.upd,sfc+18.upd,sfj+20.upd,sfl+05.upd,sh14.upd,sh82.upd,sha07.upd,sha09.upd,sha10.upd,sha98.upd,shh84.upd,sj13.upd,sjd+21.upd,sjk+21.upd,sjm14.upd,sjm+18.upd,sjm+19.upd,ska+16.upd,skc14.upd,skdl09.upd,skk11.upd,skk+80.upd,skk+97.upd,skl+15.upd,skm+17.upd,skz+19.upd,sl20.upd,sl96.upd,slb+18.upd,sle+14.upd,slr+14.upd,sls+18.upd,smb71.upd,smd93.upd,sml+01.upd,snk96.upd,sns+05.upd,snt+97.upd,snt97.upd,sof95.upd,spl13.upd,spp+23.upd,sr68.upd,srb+15.upd,srg+97.upd,srm+15.upd,srp+22.upd,srs+86.upd,ssc+19.upd,sscs19.upd,sstd86.upd,stc99.upd,sttw02.upd,stwd85.upd,svf+16.upd,sw86.upd,tacl99.upd,tamt93.upd,tau19.upd,tbb+03.upd,tbc+18.upd,tbc+20.upd,tbf+06.upd,tbms98.upd,tbr+12.upd,tck12.upd,tck13.upd,tcr+09.upd,tde+17.upd,tdk+93.upd,tfko75.upd,tgv+00.upd,th69.upd,tjb+13.upd,tjb+14.upd,tjl+17.upd,tkgg06.upd,tkk+98.upd,tkp+14.upd,tkp+22.upd,tkt22.upd,tkt+98.upd,tl06.upd,tl12.upd,tm07.upd,tml93.upd,tmp+15.upd,tnj+94.upd,to05.upd,to07.upd,tpc+11.upd,tri68.upd,trr+21.upd,tsm+19.upd,tsn+01.upd,tst+01.upd,ttd+99.upd,ttdm97.upd,ttk+17.upd,ttm+18.upd,ttm18.upd,ttol16.upd,tv68.upd,tw82.upd,umw+93.upd,ura02.upd,vazs69.upd,vb21.upd,vbc+22.upd,vbjj05.upd,vbv+08.upd,vck+10.upd,vdk+18.upd,vff+20.upd,vg97.upd,vgtg00.upd,vk08.upd,vk99.upd,vks+15.upd,vl14.upd,vl69.upd,vl70.upd,vl72.upd,vlw69.upd,vms83.upd,vrn12.upd,vwc+12.upd,waa+10.upd,wakp89.upd,wal12.upd,wan11.upd,wbb+81.upd,wbl01.upd,wcd91.upd,wck+04.upd,wcp+18.upd,wel+10.upd,wf92.upd,wg96.upd,wh16.upd,wj08.upd,wje11.upd,wjm04.upd,wkg+02.upd,wkg+11.upd,wkh+12.upd,wkm+89.upd,wkt+04.upd,wkv+99a.upd,wlc+21.upd,wlz+18.upd,wm97a.upd,wm97.upd,wmg+22.upd,wmk+22.upd,wmp+00.upd,wnt10.upd,wob+04.upd,wol90a.upd,wol91a.upd,wol91b.upd,wol94.upd,wol95.upd,wpc+90.upd,wpk+04.upd,wps+20.upd,wrts22.upd,wsc+06.upd,wu18.upd,wvl69.upd,wvumsp21.upd,wwh+20.upd,xbt+17.upd,xwh+19.upd,yhh94.upd,ymh+13.upd,ymj99.upd,ymv+11.upd,ymw+10.upd,ymw+17.upd,ymw17.upd,ypr+21.upd,ysy+13.upd,yuk04.upd,ywlw13.upd,ywml10.upd,yws+15.upd,yzc+06.upd,zav07.upd,zbcg08.upd,zcl+14.upd,zct+05.upd,zcwl96.upd,zfk+19.upd,zhi+11.upd,zhm+19.upd,zht+20.upd,zhw+05.upd,zhx+23.upd,zkgl09.upd,zkk+19.upd,zkm+11.upd,zlh+19.upd,zmc+20.upd,zpst00.upd,zrb+22.upd,zsd+15.upd,zsp05.upd,zwm+08.upd,zxw22.upd,zyw+19.upd,zzh21.upd,zzz+19.upd,cab_fixw.txt"

[ ! -e ./psrcat_update_run.log ] || /bin/rm ./psrcat_update_run.log
exec &> >(tee -a ./psrcat_update_run.log)

/bin/rm allbibs.txt

\cp psrcat2_stable.db psrcat2.db

echo "Insert version information"
sqlite3 psrcat2.db "INSERT INTO version (version_id,version,entryDate,notes) VALUES (NULL,'$VER',DATETIME('now'),'This update has been produced from work done by Agastya Kapur and Lawrence Toomey and others. This contains fixes for v2.6.1 \
 \
Software versions: Made by running .version in sqlite3\
SQLite 3.41.2 2023-03-22 11:56:21 0d1fc92f94cb6b76bffe3ec34d69cffde2924203304e8ffc4155597af0c191da \
zlib version 1.2.13 \
gcc-11.2.0 \
 \
Software collection DOI: https://doi.org/10.25919/nk2e-d839 \
 \
Git Commit Tag: \
 \
Update file labels: \
$UPDS
'\
)"


# Remove derived parameters from parameter table
sqlite3 psrcat2.db "DELETE FROM parameter where parameter_id IN (SELECT parameter_id FROM derived)"

# Clear derived and derivedFromParameter tables
sqlite3 psrcat2.db "DELETE FROM derived"
sqlite3 psrcat2.db "DELETE FROM derivedFromParameter"

# Fix citation versions (NOTE: just for v2.6.1)
sqlite3 psrcat2.db "UPDATE citation SET version_id='1' WHERE citation_id<1156"

# Update observingSystem table for systemLabel,telescope
sqlite3 psrcat2.db "UPDATE observingSystem SET telescope='MeerKAT',systemLabel='L-band receiver' WHERE (observingSystem_id=117)"
sqlite3 psrcat2.db "UPDATE parameter SET observingSystem_id=117 where observingSystem_id IN (118,119,120,121,122,123,124,125,126,127,128)"
sqlite3 psrcat2.db "DELETE FROM observingSystem WHERE observingSystem_id IN (118,119,120,121,122,123,124,125,126,127,128)"

# PSRSOFT-296
# Remove wrong ELONG ELAT values for J0406+3039 
sqlite3 psrcat2.db "DELETE FROM parameter WHERE parameter_id=46836"
sqlite3 psrcat2.db "DELETE FROM parameter WHERE parameter_id=46837"

# PSRSOFT-301
# J1843-0757
J18430757=`sqlite3 psrcat2.db "select pulsar_id from pulsar where jname LIKE 'J1843-0757'"`
J184308=`sqlite3 psrcat2.db "select pulsar_id from pulsar where jname LIKE 'J1843-08'"`
J18430757cit=`sqlite3 psrcat2.db "select citation_id from pulsar where jname LIKE 'J1843-0757'"`
# Add 0757 name to name table
sqlite3 psrcat2.db "INSERT INTO name (name_id,pulsar_id,citation_id,name,entryDate) VALUES (NULL,'$J184308','J18430757cit','J1843-0757',DATETIME('now'))"
# Update parameter table to change -0757_id to -08_id
sqlite3 psrcat2.db "UPDATE parameter SET pulsar_id='$J184308' WHERE pulsar_id='$J18430757'"
# Delete non g name
sqlite3 psrcat2.db "DELETE FROM pulsar WHERE jname LIKE 'J1843-0757'"

# PSRSOFT-281 
# Deleting period in DB.
# Insert P0 and P1 as in the pfd supplementary image
# I think just delete the P0 since the P1 measurement in the PFD is sketchy
sqlite3 psrcat2.db "DELETE FROM parameter where parameter_id=45723"
sqlite3 psrcat2.db "INSERT INTO parameter (parameter_id,pulsar_id,citation_id,parameterType_id,timeDerivative,companionNumber,referenceTime,value,uncertainty) VALUES (NULL,53,1207,13,0,0,56795.562,354.7318e-3,0.0021e-3)"
sqlite3 psrcat2.db "INSERT INTO parameter (parameter_id,pulsar_id,citation_id,parameterType_id,timeDerivative,companionNumber,referenceTime,value,uncertainty) VALUES (NULL,53,1207,13,1,0,56795.562,0.0e-7,3.1e-7)"

# Process input upd files
echo "Processing upd input files..."
for upd in `echo $UPDS | tr ',' ' '`
do
  # sanitise the hyphens
  sed -i 's/−/-/g' $upd
  sed -i 's/–/-/g' $upd
  ./inputUpdate.tcsh -upd $upd -commit 1
done

# Complete ingest process
n_error=`grep -c ERROR ./psrcat_update_run.log`
n_warn=`grep -c WARN ./psrcat_update_run.log`
echo "----------------------------------"
echo "Ingest completed."
echo "Number of ERRORs:   $n_error"
if [ $n_error -ne 0 ]; then
  grep ERROR ./psrcat_update_run.log
fi
echo "Number of WARNINGs: $n_warn"
echo "----------------------------------"

# Derive parameters
# echo "Deriving..."
time python deriveParameters.py

# # Convert to v1
# echo "Converting from v2 to v1..."
time ./psrcatV2_V1 $VER psrcat2.db > psrcat.db

echo "Done!"


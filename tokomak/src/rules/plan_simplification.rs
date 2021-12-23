use egg::*;
use datafusion::error::DataFusionError;
use crate::{plan::{TokomakLogicalPlan, JoinKeys}, Tokomak, PLAN_SIMPLIFICATION_RULES, pattern::{pattern, conditional_rule, transforming_pattern, twoway_pattern}, TokomakAnalysis};

use super::utils::*;

fn generate_simplification_rules()->Result<Vec<Rewrite<TokomakLogicalPlan, TokomakAnalysis>>, DataFusionError>{
    let predicate: Var = "?pred".parse().unwrap();
    //let col: Var = "?col".parse().unwrap();
    let l:Var ="?l".parse().unwrap();
    let r:Var = "?r".parse().unwrap();
    let lcol :Var= "?lcol".parse().unwrap();
    let rcol :Var= "?rcol".parse().unwrap();
    //let val: Var = "?val".parse().unwrap();
    let t1: Var = "?t1".parse().unwrap();
    let t2: Var = "?t2".parse().unwrap();
    let t3: Var = "?t3".parse().unwrap();
    let c1: Var = "?c1".parse().unwrap();
    let c2: Var = "?c2".parse().unwrap();
    let c3: Var = "?c3".parse().unwrap();

    let newkeys = "?newkeys".parse().unwrap();

    let keys: Var = "?keys".parse().unwrap();
    let reversed_keys = "?reversed_keys".parse().unwrap();
    let inner_keys = "?inner_keys".parse().unwrap();
    let outer_keys = "?outer_keys".parse().unwrap();
    let mut rules = vec![
        conditional_rule("plan-filter-crossjoin->innerjoin", 
            "(filter  \
                (cross_join ?l ?r ) \
                (and \
                    (= ?lcol ?rcol) \
                    ?pred \
                ) \
            )", "(filter (inner_join ?l ?r (keys ?lcol ?rcol)) ?pred)", and(is_column_from_plan(lcol, l, false), is_column_from_plan(rcol, r, false)) )?,
        //conditional_rule("filter-crossjoin-filter-predicate-pushdown", "(filter (cross_join ?l (filter ?r ?pred)) (and (= ?col ?other) ?other))", 
        //    "(filter (cross_join ?l (filter ?r (and ?pred (= ?col ?other))) (and (= ?col ?other) ?other))",
        //    column_from_plan(col, r)
        //)?,

        transforming_pattern("plan-inner-join-key-add", "(filter (inner_join ?l ?r ?keys)  (and (= ?lcol ?rcol) ?other))", "(filter (inner_join ?l ?r ?newkeys) ?other)", add_to_keys(l,r, lcol, rcol, keys,newkeys ), &[newkeys])?,
        
        pattern("plan-remove-filter-true" ,"(filter ?input true)" ,"?input")?,
        pattern("plan-replace-filter-false" , "(filter ?input false)", "(empty_relation false)")?,
        pattern("plan-merge-filters", "(filter (filter ?input ?inner_pred) ?outer_pred)", "(filter ?input (and ?inner_pred ?outer_pred))")?,

        //transforming_pattern("inner-cross-inner", "(inner_join (cross_join ?l ?r) ?other ?keys)" , "(inner_join (inner_join ?l ?r ?inner_keys) ?other ?outer_keys)", cross_to_inner_lower_join_keys(l,r,keys, inner_keys, outer_keys), &[inner_keys, outer_keys])?,
        transforming_pattern("inner-cross-inner-2", "(inner_join (cross_join ?c1 ?c2) ?r ?keys)" , "(inner_join (inner_join ?r ?c1 ?inner_keys) ?c2 ?outer_keys)", inner_join_cross_join_through(c1, c2, r,keys, inner_keys, outer_keys), &[inner_keys, outer_keys])?,
        
        pattern("plan-crossjoin-commutative", "(cross_join ?l ?r)", "(cross_join ?r ?l)")?,
        transforming_pattern("plan-innerjoin-commutative", "(inner_join ?l ?r ?keys)", "(inner_join ?r ?l ?reversed_keys)", revers_keys(keys, reversed_keys), &[reversed_keys])?,
        //conditional_rule("plan-pushdown-predicate-into-tablescan", "(filter (table_scan ) (and (= ?col ?val) ?pred)","(filter )", is_scalar(val))?,
        //transforming_pattern("plan-innerjoin->filter-crossjoin", "(inner_join ?l ?r ?keys)", "(filter (cross_join ?l ?r) ?pred)", keys_to_predicate(keys), predicate)?,
        pattern("plan-raise-filter", "(cross_join ?t1 (filter (cross_join ?t2 ?t3) ?pred))", "(filter (cross_join ?t1 (cross_join ?t2 ?t3)) ?pred)")?,
        pattern("plan-filter-raising-left", "(cross_join (filter ?l ?pred) ?r)", "(filter (cross_join ?l ?r) ?pred)")?,
        //table_scan has length of 5
        //pattern("crossjoin-tablescan-swap", "(cross_join (cross_join ?ll ?lr) (cross_join ?rl ?rr))", "(cross_join (cross_join ?ll ?rr) (cross_join ?rl ?lr))")?,
        pattern("plan-filter-raising-right", "(cross_join  ?l (filter ?r ?pred))", "(filter (cross_join ?l ?r) ?pred)")?,
        conditional_rule("plan-filter-lower-new-filter", "(filter (cross_join ?l ?r) (and (= ?c1 ?c2) ?other))","(filter (cross_join (filter ?l (and (= ?c1 ?c2) true)) ?r) ?other)",and(is_column_from_plan( c1,l, false), is_column_from_plan( c2,l, false)) )?,
        conditional_rule("plan-filter-lower-existing-filter", 
        "(filter \
            (cross_join \
                (filter ?l ?existing ) \
                ?r\
            )\
            (and \
                (= ?c1 ?c2) \
                ?other\
            )\
        )", "(filter (cross_join (filter ?l (and (= ?c1 ?c2) ?existing)) ?r) ?other)" ,and(is_column_from_plan(c1,l , false), is_column_from_plan(c2,l, false)))?
        //pattern("expr-rotate-add", "(+ ?x (+ ?y ?z))","(+ (+ ?x ?y) ?z)")?,


        //conditional_rule("plan-crossjoin-through", 
        //    "(filter (cross_join  ?t1 (cross_join ?t2 ?t3))  (and (= ?c1 ?c2) (and (= ?c2 ?c3) ?other)))",
        //    "(filter (inner_join ?t1 (inner_join ?t2 ?t3 (keys ?c2 ?c3)) (keys ?c1 ?c2)) ?other)",
        //    and(is_column_from_plan(c1, t1), and(is_column_from_plan(c2, t2), is_column_from_plan(c3, t3)))
        //)?
    ];
    rules.extend(twoway_pattern("plan-crossjoin-rotate" ,"(cross_join ?t1 (cross_join ?t2 ?t3))", "(cross_join (cross_join ?t1 ?t2) ?t3)")?);
    
    Ok(rules)
}
//"(inner_join (cross_join ?c1 ?c2) ?r ?keys)" => "(inner_join (inner_join ?c1 ?r ?inner_keys) ?c2 ?outer_keys)",
fn inner_join_cross_join_through(c1: Var, c2: Var,  r: Var,keys: Var, inner_keys: Var, outer_keys: Var)->impl Fn(&mut EGraph<TokomakLogicalPlan, TokomakAnalysis>, &mut Subst)->Option<()>{
    move |egraph: &mut EGraph<TokomakLogicalPlan, TokomakAnalysis>, subst: &mut Subst|->Option<()>{
        assert!(egraph[subst[keys]].nodes.len() ==1);
        let join_keys = &get_join_keys(egraph, subst[keys])?.0;
        let cross1_schema = get_plan_schema(egraph, subst, c1)?;
        let cross2_schema = get_plan_schema(egraph, subst, c2)?;
        let rschema = get_plan_schema(egraph, subst, r)?;
        let mut c1_r_keys = Vec::new();
        let mut inner_join_c2_keys = Vec::new();
        assert!(join_keys.len()%2==0);
        for win in join_keys.chunks_exact(2){
            let lkey = win[0];
            let rkey = win[1];
            let lcol = match &egraph[lkey].nodes[0]{
                TokomakLogicalPlan::Column(c)=> c,
                p => panic!("Found non-column value in inner join keys: {:?}", p)
            };
            let rcol = match &egraph[rkey].nodes[0]{
                TokomakLogicalPlan::Column(c)=> c,
                p => panic!("Found non-column value in inner join keys: {:?}", p)
            };
            assert!(col_from_plan(rschema, rcol), "Found right join key that was not in plan");
            if col_from_plan(cross1_schema, lcol){
                c1_r_keys.push(lkey);
                c1_r_keys.push(rkey);
            }else if col_from_plan(cross2_schema, lcol){
                inner_join_c2_keys.push(lkey);
                inner_join_c2_keys.push(rkey);
            }else{
                return None;
            }
        }
        let c1_r_keys = JoinKeys::new(c1_r_keys)?;
        let inner_join_c2_keys = JoinKeys::new(inner_join_c2_keys)?;
        let c1_r_id = egraph.add(TokomakLogicalPlan::JoinKeys(c1_r_keys));
        let inner_join_c2_id = egraph.add(TokomakLogicalPlan::JoinKeys(inner_join_c2_keys));
        subst.insert(inner_keys, c1_r_id);
        subst.insert(outer_keys, inner_join_c2_id);
        Some(())
    }
}


fn cross_to_inner_lower_join_keys(l:Var,r:Var,keys:Var, inner_keys:Var, outer_keys:Var)->impl Fn(&mut EGraph<TokomakLogicalPlan, TokomakAnalysis>, &mut Subst)->Option<()>{
    move |egraph: &mut EGraph<TokomakLogicalPlan, TokomakAnalysis>, subst: &mut Subst|->Option<()>{
        assert!(egraph[subst[keys]].nodes.len() ==1);
        let join_keys = match &egraph[subst[keys]].nodes[0]{
            TokomakLogicalPlan::JoinKeys(k)=>&k.0,
            _=> return None,
        };
        let lschema = get_plan_schema(egraph,subst, l)?;
        let rschema = get_plan_schema(egraph,subst, r)?;
        let mut inner_join_keys = Vec::new();
        let mut outer_join_keys = Vec::with_capacity(join_keys.len());
        assert!(join_keys.len() %2 ==0);
        for ids in join_keys.chunks_exact(2){
            let lkey = ids[0];
            let rkey = ids[1];
            assert!(egraph[lkey].nodes.len() == 1);
            assert!(egraph[rkey].nodes.len() == 1);
            let lcol = match &egraph[lkey].nodes[0]{
                TokomakLogicalPlan::Column(c)=> c,
                p => panic!("Found non-column value in inner join keys: {:?}", p)
            };
            let rcol = match &egraph[rkey].nodes[0]{
                TokomakLogicalPlan::Column(c)=> c,
                p => panic!("Found non-column value in inner join keys: {:?}", p)
            };
            if let (Some(_), Some(_)) = (get_field(lschema, lcol), get_field(rschema, rcol)){
                inner_join_keys.push(lkey);
                inner_join_keys.push(rkey);
            } else if let (Some(_), Some(_)) = (get_field(lschema, rcol), get_field(rschema, lcol)){
                inner_join_keys.push(rkey); 
                inner_join_keys.push(lkey);
            }else{
                outer_join_keys.push(lkey);
                outer_join_keys.push(rkey);
            }
        }
        if inner_join_keys.is_empty(){
            return None;
        }
        let inner_keys_id = egraph.add(TokomakLogicalPlan::JoinKeys(crate::plan::JoinKeys::from_vec(inner_join_keys)));
        let outer_keys_id = egraph.add(TokomakLogicalPlan::JoinKeys(crate::plan::JoinKeys::from_vec(outer_join_keys)));
        subst.insert(inner_keys, inner_keys_id);
        subst.insert(outer_keys, outer_keys_id);
        println!("Turned inner join cross into inner ");
        Some(())
    }
}

fn log_tables(table: Var)->impl Condition<TokomakLogicalPlan, TokomakAnalysis>{
    move |egraph: &mut EGraph<TokomakLogicalPlan,TokomakAnalysis>, id: Id, subst: &Subst|->bool{
        let eclass = &egraph[subst[table]];
        let scan_of_parts = eclass.nodes.iter().flat_map(|p| match p{
            TokomakLogicalPlan::TableScan(t)=>{
                egraph[t.name()].nodes.iter().flat_map(|p| match p{
                    TokomakLogicalPlan::Str(s)=>Some(s),
                    _=>None,
                }).next()
            },
            _ => None
        }).next();
        if scan_of_parts.is_some(){
            let mut inner_joins = Vec::new();
            for node in &eclass.nodes{
                let push = match node{
                    TokomakLogicalPlan::InnerJoin(_)=>true,
                    _ => false,
                };
                if push{
                    inner_joins.push(node.clone())
                }
            }
            if !inner_joins.is_empty(){
                println!("part_innerjoins: {:?}", inner_joins);
            }
        }
        false
    }
}

impl Tokomak{
    pub(crate) fn add_plan_simplification_rules(&mut self){
        let rules = generate_simplification_rules().unwrap();
        self.rules.extend(rules);
        println!("There are now {} rules", self.rules.len());
        self.added_builtins |= PLAN_SIMPLIFICATION_RULES;
    }
}

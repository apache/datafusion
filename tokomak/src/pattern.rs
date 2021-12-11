
use datafusion::{arrow::datatypes::DataType, error::DataFusionError};
use egg::{Analysis, Applier, EClass, EGraph, FromOp, Id, Language, LanguageChildren, RecExpr, RecExprParseError, SearchMatches, Searcher, Subst, Var, Rewrite, Condition, ConditionalApplier};
use std::{cmp::Ordering, collections::{BinaryHeap, HashSet}, mem::{Discriminant, discriminant}, str::FromStr, marker::PhantomData};
use crate::{Tokomak, TokomakExpr, plan::TokomakLogicalPlan};



pub enum Location{
    Before,
    After,
    BeforeAndAfter
}

//Enum containing all expression types
pub enum ExprKind{
    ScalarValue,
    Column,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PatternAst{
    ENode(TokomakLogicalPlan),
    TypedVar(CategorizedVar),
    Var(Var),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TokomakCategory{
    Plan,
    Expr,
    Column,
    List,
    FieldType,
    ScalarBuiltin,
    AggregateBuiltin,
    WindowBuiltin,
    Scalar,
    SortSpec,
    ScalarUDF,
    AggregateUDF,
    TypedColumn,
    WindowFrame,
    Cast,
    TryCast
}



impl TokomakCategory{

    fn matches(&self, plan: &TokomakLogicalPlan)->bool{
        todo!()
        
        
    }
}


pub fn transforming_pattern<A: Analysis<TokomakLogicalPlan> +'static, TR:'static+ Send+ Sync+ Fn(&mut EGraph<TokomakLogicalPlan, A>, &Subst)->Option<Id>>(name: &str, search: &str, applier: &str, transform: TR, bound_var: Var)->Result<Rewrite<TokomakLogicalPlan, A>, DataFusionError>{
    let searcher  = egg::Pattern::from_str(search).map_err(|e| DataFusionError::Plan(format!("Rule '{}' could not parse the searcher pattern : {}",name, e)))?;
    let inner_applier = egg::Pattern::from_str(applier).map_err(|e|DataFusionError::Plan(format!("Rule '{}' could not parse the applier pattern : {}",name, e)))?;
    let applier = ModifyingApplier{
        inner_applier,
        transform,
        bound_var,
        _analysis: Default::default(),
    };
    Rewrite::<TokomakLogicalPlan, A>::new(name, searcher, applier).map_err(|e| DataFusionError::Plan(format!("Rule '{}' could not create rewrite: {}", name, e)))
}

pub fn twoway_pattern<A: Analysis<TokomakLogicalPlan>>(name: &str, search: &str, applier: &str)->Result<[Rewrite<TokomakLogicalPlan,A>; 2], DataFusionError>{
    let searcher  = egg::Pattern::from_str(search).map_err(|e| DataFusionError::Plan(format!("Rule '{}' could not parse the searcher pattern : {}",name, e)))?;
    let applier = egg::Pattern::from_str(applier).map_err(|e|DataFusionError::Plan(format!("Rule '{}' could not parse the applier pattern : {}",name, e)))?;
    let mut name_buf = String::with_capacity(name.len() + 15);
    use std::fmt::Write;
    write!(name_buf, "{}_forwards", name).unwrap();
    let forwards =     Rewrite::<TokomakLogicalPlan, A>::new(name_buf.as_str(), searcher.clone(), applier.clone()).map_err(|e| DataFusionError::Plan(format!("Rule '{}' could not create rewrite: {}", name, e)))?;
    name_buf.clear();
    write!(name_buf, "{}_backwards", name).unwrap();
    let backwards =     Rewrite::<TokomakLogicalPlan, A>::new(name_buf.as_str(), searcher, applier).map_err(|e| DataFusionError::Plan(format!("Rule '{}' could not create rewrite: {}", name, e)))?;
    Ok([forwards, backwards])
}

pub fn pattern<A: Analysis<TokomakLogicalPlan>>(name: &str, search: &str, applier: &str)->Result<Rewrite<TokomakLogicalPlan,A>, DataFusionError>{
    let searcher  = egg::Pattern::from_str(search).map_err(|e| DataFusionError::Plan(format!("Rule '{}' could not parse the searcher pattern : {}",name, e)))?;
    let applier = egg::Pattern::from_str(applier).map_err(|e|DataFusionError::Plan(format!("Rule '{}' could not parse the applier pattern : {}",name, e)))?;
    Rewrite::<TokomakLogicalPlan, A>::new(name, searcher, applier).map_err(|e| DataFusionError::Plan(format!("Rule '{}' could not create rewrite: {}", name, e)))
}

pub fn conditional_rule<A: Analysis<TokomakLogicalPlan>, COND: Condition<TokomakLogicalPlan, A> + 'static + Send + Sync>(name: &str, search: &str, applier: &str, condition: COND)->Result<Rewrite<TokomakLogicalPlan, A>, DataFusionError>{
    let searcher  = egg::Pattern::from_str(search).map_err(|e| DataFusionError::Plan(format!("Rule '{}' could not parse the searcher pattern : {}",name, e)))?;
    let applier = egg::Pattern::from_str(applier).map_err(|e|DataFusionError::Plan(format!("Rule '{}' could not parse the applier pattern : {}",name, e)))?;
    let applier = ConditionalApplier{applier, condition};
    Rewrite::<TokomakLogicalPlan, A>::new(name, searcher, applier).map_err(|e| DataFusionError::Plan(format!("Rule '{}' could not create rewrite: {}", name, e)))

}

pub fn conditional_transforming_pattern<A, TR, COND>(name: &str, search: &str, applier: &str, condition: COND, transform: TR)->Result<Rewrite<TokomakLogicalPlan, A>, DataFusionError>
where A: Analysis<TokomakLogicalPlan> +'static,
TR:'static+ Send+ Sync+ Fn(&mut EGraph<TokomakLogicalPlan, A>, &Subst)->Option<Id>,
COND: Condition<TokomakLogicalPlan, A>{
    let searcher = egg::Pattern::<TokomakLogicalPlan>::from_str(search).map_err(|e| DataFusionError::Plan(format!("Rule '{}' could not parse the searcher pattern : {}",name, e)))?;
    let applier = egg::Pattern::<TokomakLogicalPlan>::from_str(applier).map_err(|e|DataFusionError::Plan(format!("Rule '{}' could not parse the applier pattern : {}",name, e)))?; 
    todo!()
}

impl FromStr for TokomakCategory{
    type Err=DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_lowercase();
        let cat = match s.as_str(){
            "plan"=>TokomakCategory::Plan,
            "col"=>TokomakCategory::Column,
            "list"=> TokomakCategory::List,
            "ty"=> TokomakCategory::FieldType,
            "scalar_builtin"=>TokomakCategory::ScalarBuiltin,
            "agg_builtin"=>TokomakCategory::AggregateBuiltin,
            "win_builtin"=>TokomakCategory::WindowBuiltin,
            "sort_spec"=>TokomakCategory::SortSpec,
            "sudf"=>TokomakCategory::ScalarUDF,
            "udaf"=>TokomakCategory::AggregateUDF,
            "win_frame"=>TokomakCategory::WindowFrame,
            "lit" => TokomakCategory::Scalar,
            s => return Err(DataFusionError::Internal(format!("Could not parse '{}' as tokomak category", s))),
        };
        Ok(cat)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CategorizedVar{
    var: Var,
    cat: TokomakCategory
}

impl FromStr for CategorizedVar{
    type Err=DataFusionError;
    //Good error messages aren't required here as they are thrown away by egg's S expr parser.
    //TODO: Implement parsing manually to allow better rule error messages to be returned
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut s =  s.split(':');
        let var_s = s.next().ok_or_else(|| DataFusionError::Internal(String::new()))?;
        let cat_s = s.next().ok_or_else(|| DataFusionError::Internal(String::new()))?;
        if s.next().is_some(){
            return Err(DataFusionError::Internal(String::new()));
        }
        let var: Var = var_s.parse().map_err(|_| DataFusionError::Internal(String::new()))?;
        let cat: TokomakCategory = cat_s.parse()?;
        Ok(CategorizedVar{var,cat})
    }
}


impl PatternAst{
    fn len(&self)->usize{
        match self{
            PatternAst::ENode(enode) => enode.len(),
            PatternAst::TypedVar(_)| 
            PatternAst::Var(_)  => 0,
        }
    }
}


impl Language for PatternAst{
    fn matches(&self, other: &Self) -> bool {
        panic!("This should never be called");
    }

    fn children(&self) -> &[Id] {
        match self{
            PatternAst::ENode(node) =>node.children(),
            PatternAst::TypedVar(_)|
            PatternAst::Var(_)  => &[],
        }
    }

    fn children_mut(&mut self) -> &mut [Id] {
        match self{
            PatternAst::ENode(node) =>node.children_mut(),
            PatternAst::TypedVar(_)|
            PatternAst::Var(_)  => &mut [],
        }
    }
}

impl FromOp for PatternAst {
    type Error = DataFusionError;

    fn from_op(op: &str, children: Vec<Id>) -> Result<Self, Self::Error> {

        if op.starts_with('?') && op.len() > 1 {
            if children.is_empty() {
                if op.contains(':'){
                    op.parse().map(Self::TypedVar).map_err(|e| DataFusionError::Internal(format!("Could not parse '{}' as categorized variable: {}", op, e)))
                }else{
                    op.parse().map(Self::Var).map_err(|e| DataFusionError::Internal(format!("Could not parse '{}' as variable: {}", op, e)))
                }
            } else {
                Err(DataFusionError::Internal(format!("Found op var that had children: '{}'", op)))
            }
        } else {
            TokomakLogicalPlan::from_op(op, children).map(Self::ENode).map_err(|e| DataFusionError::Internal(format!("Could not parse tokomak expression with op '{}': {}", op, e)))
        }
    }
}

impl std::str::FromStr for Pattern {
    type Err = RecExprParseError<DataFusionError>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ast = RecExpr::<PatternAst>::from_str(s)?;
        let program = Program::compile_from_pat(&ast);
        Ok(Pattern{
            pattern_ast:ast,
            program,
        })
    }
}

#[derive(Clone)]
pub struct Pattern{
    pattern_ast: RecExpr<PatternAst>,
    program: Program
}

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct Reg(u32);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Program {
    instructions: Vec<Instruction>,
    ground_terms: Vec<RecExpr<TokomakLogicalPlan>>,
    subst: Subst,
}

impl Program{
    fn compile_from_pat(ast: &RecExpr<PatternAst>)->Program{
        let program = Compiler::compile(ast.as_ref());
        log::debug!("Compiled {:?} to {:?}", ast.as_ref(), program);
        program

    }
    /*
    pub fn run<A: Analysis<TokomakLogicalPlan>>(&self, egraph: &EGraph<TokomakLogicalPlan, A> , eclass: Id)->Vec<Subst>{
        let mut machine = Machine::new();
        assert!(egraph.clean, "Tried to search a dirty e-graph!");
        assert_eq!(machine.reg.len(), 0);
        for expr in &self.ground_terms{
            if let Some(id) = egraph.lookup_expr(expr){
                println!("Found a a ground term");
                machine.reg.push(id)
            }else{
                println!("Found not ground terms");
                return vec![]
            }
        }
        machine.reg.push(eclass);
        let mut matches = Vec::new();
        machine.run(egraph, &self.instructions, &self.subst, &mut |machine, subst| {
            let subst_vec = subst
                .vec
                .iter()
                // HACK we are reusing Ids here, this is bad
                .map(|(v, reg_id)| (*v, machine.reg(Reg(usize::from(*reg_id) as u32))))
                .collect();
            matches.push(Subst { vec: subst_vec });
        });
        println!("The matches are {:?}", matches);
        matches


    } */
}


struct Machine{
    reg: Vec<Id>
}

//TODO: This will struggle as the number of nodes an eclass has increases. Figure out way to allow searching through nodes with category.
fn for_each_node_in_cat<D>(eclass: &EClass<TokomakLogicalPlan, D>, cat: TokomakCategory, mut f: impl FnMut(&TokomakLogicalPlan)){
    println!("Searching for category: {:?}", cat);
    let mut num_found = 0;
    for e in eclass.nodes.iter().filter(|e| cat.matches(*e)){
        f(e);
        num_found +=1;
    }
    println!("Found {} from {:?}", num_found, cat);
}

#[inline(always)]
fn for_each_matching_node<D>(eclass: &EClass<TokomakLogicalPlan, D>, node: &TokomakLogicalPlan, mut f: impl FnMut(&TokomakLogicalPlan))
{
    #[allow(clippy::mem_discriminant_non_enum)]
    if eclass.nodes.len() < 50 {
        eclass.nodes.iter().filter(|n| node.matches(n)).for_each(f)
    } else {
        debug_assert!(node.all(|id| id == Id::from(0)));
        debug_assert!(eclass.nodes.windows(2).all(|w| w[0] < w[1]));
        let mut start = eclass.nodes.binary_search(node).unwrap_or_else(|i| i);
        let discrim = std::mem::discriminant(node);
        while start > 0 {
            if std::mem::discriminant(&eclass.nodes[start - 1]) == discrim {
                start -= 1;
            } else {
                break;
            }
        }
        let matching = eclass.nodes[start..]
            .iter()
            .take_while(|&n| std::mem::discriminant(n) == discrim)
            .filter(|n| node.matches(n));
        debug_assert_eq!(
            matching.clone().count(),
            eclass.nodes.iter().filter(|n| node.matches(n)).count(),
            "matching node {:?}\nstart={}\n{:?} != {:?}\nnodes: {:?}",
            node,
            start,
            matching.clone().collect::<HashSet<_, fxhash::FxBuildHasher>>(),
            eclass
                .nodes
                .iter()
                .filter(|n| node.matches(n))
                .collect::<HashSet<_,fxhash::FxBuildHasher>>(),
            eclass.nodes
        );
        matching.for_each(&mut f);
    }
}




impl Machine{
    pub fn new()->Self{
        Self{reg: Vec::new()}
    }
    pub fn reg(&self, reg: Reg)->Id{
        self.reg[reg.0 as usize]
    }
    /* 
    fn run<A: Analysis<TokomakLogicalPlan>, Y: FnMut(&Machine, &Subst)>(&mut self, egraph: &EGraph<TokomakLogicalPlan, A>, instructions: &[Instruction], subst: &Subst, yield_fn:&mut Y){
        let mut instructions = instructions.iter();
        while let Some(ins) = instructions.next(){
            match ins{
                Instruction::ConditionalBind { kind, inp, out } => {
                    let eclass = &egraph[self.reg(*inp)];
                    if !eclass.nodes.iter().any(|e| kind.matches(e)){
                        println!("Found no nodes in category: {:?}", kind);
                        return;
                    }
                    let remaining_instructions = instructions.as_slice();
                    return for_each_node_in_cat(eclass, *kind, |matched|{
                        self.reg.truncate(out.0 as usize);
                        matched.for_each(|id| self.reg.push(id));
                        self.run(egraph,remaining_instructions, subst, yield_fn)
                    });
                },
                Instruction::Bind { inp, out, node } => {
                    println!("Executing bind");
                    let remaining_instructions = instructions.as_slice();
                    return for_each_matching_node(&egraph[self.reg(*inp)], node, |matched| {
                        self.reg.truncate(out.0 as usize);
                        matched.for_each(|id| self.reg.push(id));
                        self.run(egraph, remaining_instructions, subst, yield_fn)
                    });
                }
                Instruction::Compare { i, j } => {
                    println!("Comparing registers");
                    if egraph.find(self.reg(*i)) != egraph.find(self.reg(*j)){
                        println!("The comparison failed");
                        return;
                    }
                },
            }
        }
        println!("Yielding value");
        yield_fn(self,subst)
    }*/
}




#[derive(Debug, Clone, PartialEq, Eq)]
enum Instruction {
    ConditionalBind{kind: TokomakCategory, inp: Reg, out: Reg},
    //TypedBind{node: TokomakExpr, kind: Discriminant<TokomakExpr>, inp: Reg, out: Reg},
    Bind { node: TokomakLogicalPlan, inp: Reg, out: Reg },
    Compare { i: Reg, j: Reg },
}

#[derive(PartialEq, Eq)]
struct ToDo{
    reg: Reg,
    is_ground: bool,
    loc: usize,
    pat: PatternAst
}


impl PartialOrd for ToDo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ToDo{
    // TodoList is a max-heap, so we greater is higher priority
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self.is_ground, &other.is_ground) {
            (true, false) => return Ordering::Greater,
            (false, true) => return Ordering::Less,
            _ => (),
        };
        // fewer children means higher priority
        other.pat.len().cmp(&self.pat.len())
    }
}


struct Compiler<'a>{
    pattern: &'a [PatternAst],
    var2reg: indexmap::IndexMap<Var, Reg, fxhash::FxBuildHasher>,
    todo: BinaryHeap<ToDo>,
    out: Reg
}

impl<'a> Compiler<'a>{

    fn compile(pattern:&'a [PatternAst])->Program{
        let mut compiler = Compiler{
            pattern,
            var2reg: Default::default(),
            todo: Default::default(),
            out: Reg(0),
        };
        compiler.go()
    }
    fn go(&mut self)->Program{
        //Get all  of the ground expressions
        let mut is_ground = vec![false; self.pattern.len()];
        for i in 0..self.pattern.len(){
            if let PatternAst::ENode(node) = &self.pattern[i]{
                is_ground[i] = node.all(|c| is_ground[usize::from(c)]);
            }
        }
        let ground_locs = self.get_ground_locs(&is_ground);
        let  ground_terms: Vec<RecExpr<TokomakLogicalPlan>>  = ground_locs.iter().enumerate()
        .filter_map(|(i,r)| r.map(|r| (i,r.0)))
        .map(|(i,r)| {
            let mut expr = Default::default();
            self.build_ground_terms(i,&mut expr);
            (r, expr)
        }).map(|(_r, expr)| expr).collect();
        self.todo.push(ToDo{
            reg: Reg(self.out.0),
            is_ground: is_ground[self.pattern.len() - 1],
            loc: self.pattern.len() - 1,
            pat: self.pattern.last().unwrap().clone(),
        });
        self.out.0 += 1;
        let mut instructions: Vec<Instruction>=Vec::with_capacity(self.pattern.len());
        while let Some(ToDo{
            reg: i,
            loc,
            pat, 
            ..
        }) = self.todo.pop(){
            match pat{
                PatternAst::ENode(node) => {
                    //If the node is a ground expression, e.g. one that is made up soley of TokomakExprs then use a compare instruction
                    if let Some(j) = ground_locs[loc]{
                        instructions.push(Instruction::Compare{i,j});
                        continue;
                    }
                    let out = self.out;
                    self.out.0 += node.len() as u32;
                    let mut id: u32 = out.0;
                    node.for_each(|child|{
                        let r = Reg(id);
                        self.todo.push(ToDo { reg: r, is_ground: is_ground[usize::from(child)], loc: usize::from(child), pat: self.pattern[usize::from(child)].clone()});
                        id += 1;
                    });
                    let node = node.map_children(|_| Id::from(0));
                    instructions.push(Instruction::Bind{inp:i, node, out});
                },
                PatternAst::TypedVar(CategorizedVar{var, cat}) =>{
                    if let Some(&j) = self.var2reg.get(&var){
                        instructions.push(Instruction::Compare{i,j});
                    }else{
                        self.var2reg.insert(var,i);
                        self.out.0 +=1;
                        instructions.push(Instruction::ConditionalBind{  kind: cat, inp: i, out: self.out});
                        
                    }
                },
                PatternAst::Var(v) =>{
                    if let Some(&j) = self.var2reg.get(&v){
                        instructions.push(Instruction::Compare{i,j})
                    }else{
                        self.var2reg.insert(v, i);
                    }
                },
            }
        }
        let mut subst = Subst::default();
        for (v,r) in &self.var2reg{
            subst.insert(*v, Id::from(r.0 as usize));
        }
        Program{
            instructions,
            subst,
            ground_terms,
        }
    }

    fn build_ground_terms(&self, loc: usize, expr: &mut RecExpr<TokomakLogicalPlan>) {
        if let PatternAst::ENode(mut node) = self.pattern[loc].clone() {
            node.update_children(|c| {
                self.build_ground_terms(usize::from(c), expr);
                (expr.as_ref().len() - 1).into()
            });
            expr.add(node);
        } else {
            panic!("could only build ground terms");
        }
    }

    fn get_ground_locs(&mut self, is_ground: &[bool])->Vec<Option<Reg>>{
        let mut ground_locs: Vec<Option<Reg>> = vec![None; self.pattern.len()];
        for i in 0..self.pattern.len() {
            if let PatternAst::ENode(node) = &self.pattern[i] {
                if !is_ground[i] {
                    node.for_each(|c| {
                        let c = usize::from(c);
                        // If a ground pattern is a child of a non-ground pattern,
                        // we load the ground pattern
                        if is_ground[c] && ground_locs[c].is_none() {
                            if let PatternAst::ENode(_) = &self.pattern[c] {
                                ground_locs[c] = Some(self.out);
                                self.out.0 += 1;
                            } else {
                                unreachable!("ground locs");
                            }
                        }
                    })
                }
            }
        }
        if *is_ground.last().unwrap() {
            if self.pattern.last().is_some() {
                *ground_locs.last_mut().unwrap() = Some(self.out);
                self.out.0 += 1;
            } else {
                unreachable!("ground locs");
            }
        }
        ground_locs
    }

    
}

impl<A: Analysis<TokomakLogicalPlan>> Searcher<TokomakLogicalPlan, A> for Pattern{
    fn search_eclass(&self, egraph: &EGraph<TokomakLogicalPlan, A>, eclass: Id) -> Option<egg::SearchMatches<TokomakLogicalPlan>> {
        todo!()
       // let substs = self.program.run(egraph, eclass);
        //if substs.is_empty() {
        //    None
        //}else{
        //    Some(SearchMatches{
        //        eclass,
        //        substs,
        //        ast: None,
        //    })
        //}
    }

    fn vars(&self) -> Vec<Var> {
        let mut vars = vec![];
        for n in self.pattern_ast.as_ref(){
            match n{
                PatternAst::TypedVar(CategorizedVar{var,..})=> vars.push(*var),
                PatternAst::Var(v)=> vars.push(*v),
                _=>(),
            }
        }
        vars
    }
}

fn apply_pat< A: Analysis<TokomakLogicalPlan>>(
    ids: &mut [Id],
    pat: &[PatternAst],
    egraph: &mut EGraph<TokomakLogicalPlan, A>,
    subst: &Subst,
) -> Id {
    debug_assert_eq!(pat.len(), ids.len());
    for (i, pat_node) in pat.iter().enumerate() {
        let id = match pat_node {
            PatternAst::TypedVar(CategorizedVar{var, cat})=>{
                subst[*var]
            }
            PatternAst::Var(w) => subst[*w],
            PatternAst::ENode(e) => {
                let n = e.clone().map_children(|child| ids[usize::from(child)]);
                println!("adding: {:?}", n);
                egraph.add(n)
            }
        };
        ids[i] = id;
    }

    *ids.last().unwrap()
}
impl<A: Analysis<TokomakLogicalPlan>> Applier<TokomakLogicalPlan, A> for Pattern{
    fn apply_one(
        &self,
        egraph: &mut EGraph<TokomakLogicalPlan, A>,
        eclass: Id,
        subst: &Subst,
        searcher_ast: Option<&egg::PatternAst<TokomakLogicalPlan>>,
        rule_name: egg::Symbol,
    ) -> Vec<Id> {
        println!("Applying pattern: {:?}", self.pattern_ast);
        let pat = self.pattern_ast.as_ref();
        let mut id_buf = vec![0.into(); pat.len()];
        let id = apply_pat(&mut id_buf, pat, egraph, subst);
        if egraph.union(eclass, id){
            println!("Found alternative expr");
            vec![eclass]
        }else{
            vec![]
        }
    }
}

pub struct ModifyingApplier<A, APP, TR> 
where
A: Analysis<TokomakLogicalPlan>,
APP: Applier<TokomakLogicalPlan, A>,
TR: Fn(&mut EGraph<TokomakLogicalPlan, A>, &Subst)->Option<Id>,{
    inner_applier: APP,
    transform: TR,
    bound_var: Var,
    _analysis: PhantomData<fn()-> A>

}
impl <A, APP, TR>  ModifyingApplier<A, APP, TR> where 
A: Analysis<TokomakLogicalPlan>,
APP: Applier<TokomakLogicalPlan, A>,
TR: Fn(&mut EGraph<TokomakLogicalPlan, A>, &Subst)->Option<Id>{
    fn apply_transform(&self, egraph: &mut EGraph<TokomakLogicalPlan, A>, subst: &Subst)->Option<Id>{
        (self.transform)(egraph, subst)
    }
}


impl<A, APP, TR> Applier<TokomakLogicalPlan, A> for ModifyingApplier<A, APP, TR> where 
A:  Analysis<TokomakLogicalPlan>,
APP:  Applier<TokomakLogicalPlan,A>,
TR: Fn(&mut EGraph<TokomakLogicalPlan, A>, &Subst)->Option<Id>{
    fn apply_one(
        &self,
        egraph: &mut EGraph<TokomakLogicalPlan, A>,
        eclass: Id,
        subst: &Subst,
        searcher_ast: Option<&egg::PatternAst<TokomakLogicalPlan>>,
        rule_name: egg::Symbol,
    ) -> Vec<Id> {
        let new_var_id: Id = match self.apply_transform(egraph, subst){
            Some(id)=>id,
            None => return vec![]  
        };
        let mut new_subst = subst.clone();
        new_subst.insert(self.bound_var, new_var_id);
        self.inner_applier.apply_one(egraph, eclass, &new_subst, searcher_ast, rule_name)

    }
    //This applier slightly lies about what variables it binds, since it creates one of the bound vars for the inner applier
    fn vars(&self) -> Vec<Var> {
        self.inner_applier.vars().into_iter().filter(|v| self.bound_var != *v).collect()
    }
}

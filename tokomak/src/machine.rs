use egg::*;

use crate::{plan::TokomakLogicalPlan, pattern::TokomakCategory};

type HashSet<K> = std::collections::HashSet<K, fxhash::FxBuildHasher>;
type IndexMap<K,V> = indexmap::IndexMap<K, V,fxhash::FxBuildHasher>;
type HashMap<K, V> = std::collections::HashMap<K,V, fxhash::FxBuildHasher>;
#[derive(Default)]
struct Machine {
    reg: Vec<Id>,
    // a buffer to re-use for lookups
    lookup: Vec<Id>,
}

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct Reg(u32);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Program {
    instructions: Vec<Instruction>,
    subst: Subst,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Instruction {
    Bind { node: TokomakLogicalPlan, i: Reg, out: Reg },
    //TypedBind{i: Reg, out: Reg, cat: TokomakCategory},
    Compare { i: Reg, j: Reg },
    Lookup { term: Vec<ENodeOrReg<TokomakLogicalPlan>>, i: Reg },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ENodeOrReg<L> {
    ENode(L),
    Reg(Reg),
}

#[inline(always)]
fn for_each_matching_node<L, D>(eclass: &EClass<L, D>, node: &L, mut f: impl FnMut(&L))
where
    L: Language,
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
            matching.clone().collect::<HashSet<_>>(),
            eclass
                .nodes
                .iter()
                .filter(|n| node.matches(n))
                .collect::<HashSet<_>>(),
            eclass.nodes
        );
        matching.for_each(&mut f);
    }
}

impl Machine {
    #[inline(always)]
    fn reg(&self, reg: Reg) -> Id {
        self.reg[reg.0 as usize]
    }

    fn run<N>(
        &mut self,
        egraph: &EGraph<TokomakLogicalPlan, N>,
        instructions: &[Instruction],
        subst: &Subst,
        yield_fn: &mut impl FnMut(&Self, &Subst),
    ) where
        N: Analysis<TokomakLogicalPlan>,
    {
        let mut instructions = instructions.iter();
        while let Some(instruction) = instructions.next() {
            match instruction {
                Instruction::Bind { i, out, node } => {
                    let remaining_instructions = instructions.as_slice();
                    return for_each_matching_node(&egraph[self.reg(*i)], node, |matched| {
                        self.reg.truncate(out.0 as usize);
                        matched.for_each(|id| self.reg.push(id));
                        self.run(egraph, remaining_instructions, subst, yield_fn)
                    });
                }
                Instruction::Compare { i, j } => {
                    if egraph.find(self.reg(*i)) != egraph.find(self.reg(*j)) {
                        return;
                    }
                }

                Instruction::Lookup { term, i } => {
                    self.lookup.clear();
                    for node in term {
                        match node {
                            ENodeOrReg::ENode(node) => {
                                let look = |i| self.lookup[usize::from(i)];
                                match egraph.lookup(node.clone().map_children(look)) {
                                    Some(id) => self.lookup.push(id),
                                    None => return,
                                }
                            }
                            ENodeOrReg::Reg(r) => {
                                self.lookup.push(egraph.find(self.reg(*r)));
                            }
                        }
                    }

                    let id = egraph.find(self.reg(*i));
                    if self.lookup.last().copied() != Some(id) {
                        return;
                    }
                }
            }
        }

        yield_fn(self, subst)
    }
}

struct Compiler<'a> {
    pattern: &'a PatternAst<TokomakLogicalPlan>,
    v2r: IndexMap<Var, Reg>,
    free_vars: Vec<HashSet<Var>>,
    todo_nodes: HashMap<Id, (Reg, TokomakLogicalPlan)>,
    instructions: Vec<Instruction>,
}

impl<'a> Compiler<'a> {
    fn new(pattern: &'a PatternAst<TokomakLogicalPlan>) -> Self {
        let len = pattern.as_ref().len();
        let mut free_vars: Vec<HashSet<Var>> = Vec::with_capacity(len);

        for node in pattern.as_ref() {
            let mut free = HashSet::default();
            match node {
                ENodeOrVar::ENode(n) => {
                    for &child in n.children() {
                        free.extend(&free_vars[usize::from(child)])
                    }
                }
                ENodeOrVar::Var(v) => {
                    free.insert(*v);
                }
            }
            free_vars.push(free)
        }

        Self {
            pattern,
            free_vars,
            v2r: IndexMap::default(),
            todo_nodes: Default::default(),
            instructions: Default::default(),
        }
    }

    fn add_todo(&mut self, id: Id, reg: Reg) {
        match &self.pattern[id] {
            ENodeOrVar::Var(v) => {
                if let Some(&j) = self.v2r.get(v) {
                    self.instructions.push(Instruction::Compare { i: reg, j })
                } else {
                    self.v2r.insert(*v, reg);
                }
            }
            ENodeOrVar::ENode(pat) => {
                self.todo_nodes.insert(id, (reg, pat.clone()));
            }
        }
    }

    fn next(&mut self) -> Option<(Id, (Reg, TokomakLogicalPlan))> {
        // we take the max todo according to this key
        // - prefer grounded
        // - prefer variables
        // - prefer more free vars (if not grounded)
        let key = |id: &&Id| {
            let n_free = self.free_vars[usize::from(**id)]
                .iter()
                .filter(|v| self.v2r.contains_key(*v))
                .count() as isize;
            (n_free == 0, n_free)
        };

        self.todo_nodes
            .keys()
            .max_by_key(key)
            .copied()
            .map(|id| (id, self.todo_nodes.remove(&id).unwrap()))
    }

    /// check to see if this e-node corresponds to a term that is grounded by
    /// the variables bound at this point
    fn is_ground_now(&self, id: Id) -> bool {
        self.free_vars[usize::from(id)]
            .iter()
            .all(|v| self.v2r.contains_key(v))
    }

    fn compile(mut self) -> Program {
        let last_i = self.pattern.as_ref().len() - 1;
        let mut next_out = Reg(1);

        self.add_todo(Id::from(last_i), Reg(0));

        while let Some((id, (reg, node))) = self.next() {
            if self.is_ground_now(id) && !node.is_leaf() {
                let extracted = self.pattern.extract(id);
                self.instructions.push(Instruction::Lookup {
                    i: reg,
                    term: extracted
                        .as_ref()
                        .iter()
                        .map(|n| match n {
                            ENodeOrVar::ENode(n) => ENodeOrReg::ENode(n.clone()),
                            ENodeOrVar::Var(v) => {
                                ENodeOrReg::Reg(self.v2r[v])
                            },
                        })
                        .collect(),
                });
            } else {
                let out = next_out;
                next_out.0 += node.len() as u32;

                // zero out the children so Bind can use it to sort
                let op = node.clone().map_children(|_| Id::from(0));
                self.instructions.push(Instruction::Bind {
                    i: reg,
                    node: op,
                    out,
                });

                for (i, &child) in node.children().iter().enumerate() {
                    self.add_todo(child, Reg(out.0 + i as u32));
                }
            }
        }

        let mut subst = Subst::default();
        for (v, r) in self.v2r {
            subst.insert(v, Id::from(r.0 as usize));
        }
        Program {
            instructions: self.instructions,
            subst,
        }
    }
}

impl Program {
    pub(crate) fn compile_from_pat(pattern: &PatternAst<TokomakLogicalPlan>) -> Self {
        let program = Compiler::new(pattern).compile();
        log::debug!("Compiled {:?} to {:?}", pattern.as_ref(), program);
        program
    }

    pub fn run<A>(&self, egraph: &EGraph<TokomakLogicalPlan, A>, eclass: Id) -> Vec<Subst>
    where
        A: Analysis<TokomakLogicalPlan>,
    {
        let mut machine = Machine::default();

        assert!(egraph.clean, "Tried to search a dirty e-graph!");
        assert_eq!(machine.reg.len(), 0);
        machine.reg.push(eclass);

        let mut matches = Vec::new();
        machine.run(
            egraph,
            &self.instructions,
            &self.subst,
            &mut |machine, subst| {
                let subst_vec = subst
                    .vec
                    .iter()
                    // HACK we are reusing Ids here, this is bad
                    .map(|(v, reg_id)| (*v, machine.reg(Reg(usize::from(*reg_id) as u32))))
                    .collect();
                matches.push(Subst { vec: subst_vec });
            },
        );

        log::trace!("Ran program, found {:?}", matches);
        matches
    }
}

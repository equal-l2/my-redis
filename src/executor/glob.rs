use std::ops::RangeInclusive;

use memchr::memmem;

#[derive(Clone, Debug)]
enum Node {
    Chars(Vec<u8>),
    Question,
    Star,
    Pat(Vec<PatElement>),
    NoPat(Vec<PatElement>),
}

impl Node {
    const fn is_star(&self) -> bool {
        matches!(self, Node::Star)
    }
}

#[derive(Clone, Debug)]
enum PatElement {
    Char(u8),
    Range(RangeInclusive<u8>),
}

#[allow(private_interfaces)]
#[derive(Debug)]
pub enum Finder {
    NoMatch,
    AllMatch,
    AllMatchWithLen(usize),
    SimpleMatch(Vec<u8>),
    FindNeedle(Vec<u8>),
    RequiresMatch(Vec<Node>),
}

impl Finder {
    pub fn new(pattern: &[u8]) -> Finder {
        let Some(nodes) = compile_pattern(pattern) else {
            return Finder::NoMatch;
        };

        if nodes.len() == 1 {
            match &nodes[0] {
                Node::Star => return Finder::AllMatch,
                Node::Chars(v) => return Finder::SimpleMatch(v.to_vec()),
                _ => {}
            }
        }

        if nodes.len() == 3 {
            if let [Node::Star, Node::Chars(v), Node::Star] = &nodes[0..3] {
                return Finder::FindNeedle(v.clone());
            }
        }

        if nodes
            .iter()
            .all(|node| matches!(node, Node::Question | Node::Star))
        {
            let qs = nodes
                .iter()
                .filter(|node| matches!(node, Node::Question))
                .count();
            if qs == 0 {
            } else {
                return Finder::AllMatchWithLen(qs);
            }
        }

        Finder::RequiresMatch(nodes)
    }

    pub fn do_match(&self, input: &[u8]) -> bool {
        debug_assert!(!input.is_empty());
        match self {
            Finder::NoMatch => false,
            Finder::AllMatch => true,
            Finder::AllMatchWithLen(n) => input.len() == *n,
            Finder::SimpleMatch(v) => input == v,
            Finder::FindNeedle(f) => memmem::find(input, f).is_some(),
            Finder::RequiresMatch(nodes) => self.run_node(input, nodes),
        }
    }

    fn run_node(&self, input: &[u8], nodes: &[Node]) -> bool {
        let mut node_it = nodes.iter();
        let mut current_node = node_it.next();
        let mut skip = false;

        let mut pos = 0;
        loop {
            let Some(node) = current_node else {
                break;
            };

            if node.is_star() {
                skip = true;
                current_node = node_it.next();
                continue;
            }

            'skipping: loop {
                if pos >= input.len() {
                    return false;
                }

                match node {
                    Node::Star => unreachable!(),
                    Node::Chars(pat) => {
                        let Some(slice) = input.get(pos..pat.len()) else {
                            return false;
                        };
                        if slice == pat {
                            // OK
                            pos += pat.len();
                            current_node = node_it.next();
                            break 'skipping;
                        } else {
                            if !skip {
                                return false;
                            }
                            pos += 1;
                        }
                    }
                    Node::Question => {
                        // always OK
                        pos += 1;
                        current_node = node_it.next();
                        break 'skipping;
                    }
                    Node::Pat(pat) => {
                        let mut ok = false;
                        for el in pat {
                            match el {
                                PatElement::Char(c) => {
                                    if input[pos] == *c {
                                        ok = true;
                                        break;
                                    }
                                }
                                PatElement::Range(range) => {
                                    if range.contains(&input[pos]) {
                                        ok = true;
                                        break;
                                    }
                                }
                            }
                        }
                        if ok {
                            // OK
                            pos += 1;
                            current_node = node_it.next();
                            break 'skipping;
                        } else if skip {
                            return false;
                        }
                    }

                    Node::NoPat(pat) => {
                        let mut ok = false;
                        for el in pat {
                            match el {
                                PatElement::Char(c) => {
                                    if input[pos] == *c {
                                        ok = true;
                                        break;
                                    }
                                }
                                PatElement::Range(range) => {
                                    if range.contains(&input[pos]) {
                                        ok = true;
                                        break;
                                    }
                                }
                            }
                        }
                        if ok {
                            // OK
                            pos += 1;
                            current_node = node_it.next();
                            break 'skipping;
                        } else if skip {
                            return false;
                        }
                    }
                }
            }

            skip = false;
        }

        skip || current_node.is_none() && pos == input.len()
    }
}

fn compile_pattern(pattern: &[u8]) -> Option<Vec<Node>> {
    if pattern.is_empty() {
        return None;
    }

    let mut nodes = Vec::new();
    let mut buffer = Vec::new();
    let mut it = pattern.iter();
    loop {
        let Some(ch) = it.next() else {
            if !buffer.is_empty() {
                nodes.push(Node::Chars(buffer));
            }
            return Some(optimise_nodes(nodes));
        };
        match ch {
            b'*' => {
                if !buffer.is_empty() {
                    nodes.push(Node::Chars(std::mem::take(&mut buffer)));
                }
                nodes.push(Node::Star);
            }
            b'?' => {
                if !buffer.is_empty() {
                    nodes.push(Node::Chars(std::mem::take(&mut buffer)));
                }
                nodes.push(Node::Question);
            }
            b'[' => {
                if !buffer.is_empty() {
                    nodes.push(Node::Chars(std::mem::take(&mut buffer)));
                }
                // read in-bracket chars
                loop {
                    match it.next() {
                        Some(b'\\') => {
                            let ch_opt = it.next();
                            match ch_opt {
                                Some(b'[') => {
                                    // escaping an open bracket
                                    buffer.push(b'[');
                                }
                                Some(ch) => {
                                    buffer.extend([b'\\', *ch]);
                                }
                                None => {
                                    break;
                                }
                            }
                        }
                        Some(b'-') => {
                            // accept all chars as plain chars
                            buffer.push(b'-');
                            if let Some(ch) = it.next() {
                                buffer.push(*ch);
                            }
                        }
                        Some(b']') | None => break,
                        Some(ch) => buffer.push(*ch),
                    }
                }
                if buffer.is_empty() {
                    // "[]" matches nothing
                    return None;
                }
                if buffer == [b'^'] {
                    // "[^]" matches any single char
                    // which is the same as "?"
                    nodes.push(Node::Question);
                    buffer.clear();
                } else {
                    nodes.push(compile_bracket(buffer.drain(..))?);
                }
            }
            b'\\' => {
                const SPECIALS: &[u8] = &[b'*', b'?', b'['];
                match it.next() {
                    Some(ch) if SPECIALS.contains(ch) => buffer.push(*ch),
                    Some(ch) => buffer.extend([b'\\', *ch]),
                    None => buffer.push(b'\\'),
                }
            }
            _ => buffer.push(*ch),
        }
    }
}

fn optimise_nodes(nodes: Vec<Node>) -> Vec<Node> {
    loop {
        if nodes.len() <= 1 {
            return nodes;
        }
        let mut changed = false;
        let mut new_nodes = Vec::new();
        let mut i = 0;
        while i < nodes.len() - 1 {
            match (&nodes[i], &nodes[i + 1]) {
                (Node::Star, Node::Star) => {
                    new_nodes.push(Node::Star);
                    changed = true;
                    i += 2;
                }
                (Node::Chars(v1), Node::Chars(v2)) => {
                    let new_chars = v1.iter().chain(v2.iter()).cloned().collect();
                    new_nodes.push(Node::Chars(new_chars));
                    changed = true;
                    i += 2;
                }
                _ => {
                    new_nodes.push(nodes[i].clone());
                    i += 1;
                }
            }
        }

        if !changed {
            return nodes;
        }
    }
}

fn compile_bracket(inner: impl Iterator<Item = u8>) -> Option<Node> {
    let mut it = inner.peekable();
    let mut elems = Vec::new();
    let mut buffer = Vec::new();
    let inverted = if it.peek() == Some(&b'^') {
        it.next().unwrap();
        true
    } else {
        false
    };
    loop {
        let Some(ch) = it.next() else {
            elems.extend(buffer.drain(..).map(PatElement::Char));
            return Some(if inverted {
                Node::NoPat(elems)
            } else {
                Node::Pat(elems)
            });
        };
        match ch {
            b'-' => {
                if let Some(ch2) = buffer.pop() {
                    elems.extend(buffer.drain(..).map(PatElement::Char));

                    let pat_start = ch2;
                    if let Some(ch3) = it.next() {
                        let pat_end = ch3;
                        let range = if pat_start > pat_end {
                            pat_end..=pat_start
                        } else {
                            pat_start..=pat_end
                        };
                        elems.push(PatElement::Range(range));
                    } else {
                        // "<char>-" matches nothing
                        return None;
                    }
                } else {
                    // without starting char, "-" simply matches "-"
                    buffer.push(b'-');
                }
            }
            ch => buffer.push(ch),
        }
    }
}

#[cfg(test)]
mod test {
    use super::Finder;

    fn helper(pattern: &[u8], input: &[u8]) -> bool {
        let parser = Finder::new(pattern);
        eprintln!("{:?}", parser);
        parser.do_match(input)
    }

    #[test]
    fn test_simple_match() {
        assert!(helper(b"a", b"a"));
        assert!(!helper(b"a", b"b"));
        assert!(!helper(b"a", b"ab"));
        assert!(!helper(b"ab", b"a"));
        assert!(!helper(b"ab", b"b"));
        assert!(helper(b"ab", b"ab"));
    }

    #[test]
    fn test_star() {
        assert!(helper(b"*", b"a"));
        assert!(helper(b"*", b"b"));
        assert!(helper(b"*", b"ab"));
        assert!(helper(b"a*", b"a"));
        assert!(!helper(b"a*", b"b"));
        assert!(helper(b"a*", b"ab"));
        assert!(helper(b"*a", b"a"));
        assert!(!helper(b"*a", b"b"));
        assert!(!helper(b"*a", b"ab"));
        assert!(helper(b"*a*", b"a"));
        assert!(!helper(b"*a*", b"b"));
        assert!(helper(b"*a*", b"ab"));
    }

    #[test]
    fn test_escape() {
        assert!(helper(b"a\\*", b"a*"));
        assert!(!helper(b"a\\*", b"abc"));
    }
}

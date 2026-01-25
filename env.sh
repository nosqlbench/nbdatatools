PATH=$(pwd)/bin:$PATH
source <(nbvectors generate-completion)
complete -o nospace -C 'nbvectors complete 2>/dev/null' nbvectors


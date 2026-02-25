FROM registry.fedoraproject.org/fedora-minimal:42

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=1000

RUN microdnf -y update && \
    microdnf -y install \
      bash-completion \
      ca-certificates \
      curl \
      git \
      sudo \
      tar \
      which \
      shadow-utils \
      procps-ng \
      less \
      gcc gcc-c++ gdb make cmake ninja-build \
      python3 python3-pip python3-virtualenv python3-devel \
      nodejs npm \
      graphviz \
      java-latest-openjdk \
      plantuml \
      freetype libpng \
      gh \
      sshd \
    && microdnf -y clean all

RUN python3 -m pip install poetry && \
    poetry config virtualenvs.in-project true

RUN groupadd --gid "${USER_GID}" "${USERNAME}" \
 && useradd  --uid "${USER_UID}" --gid "${USER_GID}" -m "${USERNAME}" \
 && echo "${USERNAME} ALL=(root) NOPASSWD:ALL" > /etc/sudoers.d/90-${USERNAME} \
 && chmod 0440 /etc/sudoers.d/90-${USERNAME}

USER ${USERNAME}
WORKDIR /workspaces
